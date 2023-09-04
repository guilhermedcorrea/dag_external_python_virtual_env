from __future__ import annotations
import os
from pprint import pprint
from pendulum import datetime
from airflow.decorators import dag, task


@dag(
    start_date=datetime(2023, 10, 10),
    schedule=None,
    catchup=False,
    tags=["pythonvirtualenv"],
)
def dag_virtual_env_teste():
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """teste context"""
        pprint(kwargs)
        print(ds)
        return "retorno"

    @task.external_python(
        task_id="external_python", python="/home/guilherme/pythonjobs/venv/bin/python3.11"
    )
    def callable_external_python():
        import numpy as np
        from sklearn.linear_model import SGDClassifier
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import make_pipeline
        X = np.array([[-1, -1], [-2, -1], [1, 1], [2, 1]])
        Y = np.array([1, 1, 2, 2])
       
        clf = make_pipeline(StandardScaler(), 
                            SGDClassifier(max_iter=1000, tol=1e-3))
        clf.fit(X, Y)
        print(clf.predict([[-0.8, -1]]))


    
    task_print = print_context()
    task_external_python = callable_external_python()

    task_print >> task_external_python


dag_virtual_env_teste()