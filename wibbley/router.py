import functools
import inspect
from typing import Coroutine


class Router(object):
    def __init__(self):
        self.routes = {}

    def _get_wrapper(self, func: Coroutine):
        @functools.wraps(func)
        async def wrapper(**kwargs):
            signature = inspect.signature(func)
            parameters = signature.parameters
            param_names = [param.name for param in parameters.values()]
            new_kwargs = {}
            for param_name in param_names:
                if param_name in kwargs:
                    new_kwargs[param_name] = kwargs[param_name]

            result = await func(**new_kwargs)
            return result

        return wrapper

    def get(self, path):
        def decorator(func: Coroutine):
            wrapper = self._get_wrapper(func)
            if self.routes.get(path):
                self.routes[path]["GET"] = wrapper
                self.routes[path]["HEAD"] = wrapper
            else:
                self.routes[path] = {"GET": wrapper}
                self.routes[path]["HEAD"] = wrapper
            return wrapper

        return decorator

    def post(self, path):
        def decorator(func):
            wrapper = self._get_wrapper(func)
            if self.routes.get(path):
                self.routes[path]["POST"] = wrapper
            else:
                self.routes[path] = {"POST": wrapper}
            return wrapper

        return decorator

    def put(self, path):
        def decorator(func):
            wrapper = self._get_wrapper(func)
            if self.routes.get(path):
                self.routes[path]["PUT"] = wrapper
            else:
                self.routes[path] = {"PUT": wrapper}
            return wrapper

        return decorator

    def delete(self, path):
        def decorator(func):
            wrapper = self._get_wrapper(func)
            if self.routes.get(path):
                self.routes[path]["DELETE"] = wrapper
            else:
                self.routes[path] = {"DELETE": wrapper}
            return wrapper

        return decorator

    def patch(self, path):
        def decorator(func):
            wrapper = self._get_wrapper(func)
            if self.routes.get(path):
                self.routes[path]["PATCH"] = wrapper
            else:
                self.routes[path] = {"PATCH": wrapper}
            return func

        return decorator
