# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import re

from airflow import configuration, settings
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.settings import Session
from airflow.www import app as application
from datetime import datetime, timedelta

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_tests'

class TestKnownEventView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/knownevent/new/?url=/admin/knownevent/'

    @classmethod
    def setUpClass(cls):
        super(TestKnownEventView, cls).setUpClass()
        session = Session()
        session.query(models.KnownEvent).delete()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        cls.user_id = user.id
        session.close()

    def setUp(self):
        super(TestKnownEventView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.known_event = {
            'label': 'event-label',
            'event_type': '1',
            'start_date': '2017-06-05 12:00:00',
            'end_date': '2017-06-05 13:00:00',
            'reported_by': self.user_id,
            'description': '',
        }

    def tearDown(self):
        self.session.query(models.KnownEvent).delete()
        self.session.commit()
        self.session.close()
        super(TestKnownEventView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestKnownEventView, cls).tearDownClass()

    def test_create_known_event(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.KnownEvent).count(), 1)

    def test_create_known_event_with_end_data_earlier_than_start_date(self):
        self.known_event['end_date'] = '2017-06-05 11:00:00'
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertIn(
            'Field must be greater than or equal to Start Date.',
            response.data.decode('utf-8'),
        )
        self.assertEqual(self.session.query(models.KnownEvent).count(), 0)


class TestPoolModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/pool/new/?url=/admin/pool/'

    @classmethod
    def setUpClass(cls):
        super(TestPoolModelView, cls).setUpClass()
        session = Session()
        session.query(models.Pool).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestPoolModelView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.pool = {
            'pool': 'test-pool',
            'slots': 777,
            'description': 'test-pool-description',
        }

    def tearDown(self):
        self.session.query(models.Pool).delete()
        self.session.commit()
        self.session.close()
        super(TestPoolModelView, self).tearDown()

    def test_create_pool(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_same_name(self):
        # create test pool
        self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        # create pool with the same name
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('Already exists.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_empty_name(self):
        self.pool['pool'] = ''
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('This field is required.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 0)


class ClearViewTests(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.args = {
            'owner': 'airflow', 'start_date': DEFAULT_DATE,
            'depends_on_past': False, 'retries': 3}
        self.dagbag = app.dagbag

    def test_dag_clear_view_with_descendants(self):
        def _run_dag(dag):
            for task in dag.topological_sort():
                task.run(
                    start_date=DEFAULT_DATE,
                    end_date=DEFAULT_DATE + timedelta(seconds=1),
                    ignore_ti_state=True)

        def _parse_view(html):
            # If the way task instances are serialized changes (str(TaskInstance))
            # this regexp won't work anymore but I cannot find another way to
            # parse the html
            pattern = r"TaskInstance:\s+(\w+)\.(\w+)\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[\w+\]"
            return re.findall(pattern, html)

        dag_core_id = TEST_DAG_ID + '_core'
        dag_core = models.DAG(
            dag_core_id, default_args=self.args,
            schedule_interval=timedelta(seconds=1))
        with dag_core:
            task_core = DummyOperator(task_id='task_core')

        dag_first_child_id = TEST_DAG_ID + '_first_child'
        dag_first_child = models.DAG(
            dag_first_child_id, default_args=self.args,
            schedule_interval=timedelta(seconds=1))
        with dag_first_child:
            t1_first_child = ExternalTaskSensor(
                task_id='t1_first_child',
                external_dag_id=dag_core_id,
                external_task_id='task_core',
                poke_interval=1)
            t2_first_child = DummyOperator(
                task_id='t2_first_child')
            t1_first_child >> t2_first_child

        dag_second_child_id = TEST_DAG_ID + '_second_child'
        dag_second_child = models.DAG(
            dag_second_child_id, default_args=self.args,
            schedule_interval=timedelta(seconds=1))
        with dag_second_child:
            t1_second_child = ExternalTaskSensor(
                task_id='t1_second_child',
                external_dag_id=dag_first_child_id,
                external_task_id='t2_first_child',
                poke_interval=1)
            t2_second_child = DummyOperator(
                task_id='t2_second_child',
                dag=dag_second_child)


        all_dags = [dag_core, dag_first_child, dag_second_child]
        all_dag_ids = [dag.dag_id for dag in all_dags]
        all_task_ids = [task_id for dag in all_dags for task_id in dag.task_ids]

        for dag in all_dags:
            self.dagbag.bag_dag(dag, dag, dag)
            _run_dag(dag)
        to_rerun_task_ids = [ task_id for task_id in all_task_ids if task_id != 't2_second_child']
        session = settings.Session()
        TI = models.TaskInstance
        tis = session.query(TI).filter(TI.dag_id.in_(all_dag_ids),
            TI.task_id.in_(to_rerun_task_ids)).all()
        tis = [(ti.dag_id, ti.task_id, str(ti.execution_date)) for ti in tis]
        url = (
            "/admin/airflow/clear?task_id=task_core&"
            "dag_id={}&future=true&past=false&"
            "upstream=false&downstream=true&recursive=true&"
            "descendants=true&execution_date={}&"
            "origin=/admin".format(dag_core_id, DEFAULT_DATE))
        response = self.app.get(url)
        html = response.data.decode('utf-8')
        tasks = _parse_view(html)
        self.assertEqual(sorted(tis), sorted(tasks))

if __name__ == '__main__':
    unittest.main()
