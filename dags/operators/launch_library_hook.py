from airflow.hooks.base_hook import BaseHook


class LaunchLibraryHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = object()  # create connection instance here
            return self._conn

    def do_stuff(self, arg1, arg2, **kwargs):
        session = self.get_conn()


# class HelloOperator(BaseOperator):
#
#     @apply_defaults
#     def __init__(
#             self,
#             name: str,
#             *args, **kwargs) -> None:
#         super().__init__(*args, **kwargs)
#         self.name = name
#
#     def execute(self, context):
#         message = "Hello {}".format(self.name)
#         print(message)
#         return message
