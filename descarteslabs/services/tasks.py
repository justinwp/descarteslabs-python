import os
import requests
import time
from .service import Service

class AsyncTask(dict):
    def __init__(self, task_id=None):
        self.task_id = task_id

class AsyncTasks(Service):
    def __init__(self, url=None, token=None):
        if url is None:
            url = os.environ.get("DESCARTESLABS_TASKS_URL", "https://platform-services.descarteslabs.com/raster/dev")

        Service.__init__(self, url, token)

    def get_task(self, task):
        if not isinstance(task, AsyncTask):
            raise ValueError("`task` {} argument is not of type `AsyncTask`".format(task))

        params = {
            'task_id': task.task_id
        }
        r = self.session.get("/get_task", json=params)
        return r

    def collect(self, tasks):
        results = []
        total_tasks = len(tasks)
        while len(tasks) > 0:
            for task in tasks:
                try:
                    result_url = self.get_task(task)
                except:
                    continue

                if result_url.status_code != 200:
                    continue

                result = requests.get(result_url.content)
                task['result'] = result.content
                results.append(task)
                tasks.remove(task)

            print("Done with %i / %i tasks" % (len(results), total_tasks))
            time.sleep(1)
        return results
