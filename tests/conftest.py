import os
os.environ["SECRET_KEY"] = "testsecretkey123"
os.environ["NEXPOS_FERNET_KEY"] = "I9StON-hofzi783VWEhFYFM1DCXGJc08SBE1olJhDqI="

import httpx
from httpx import ASGITransport

original_init = httpx.Client.__init__

def patched_init(self, *args, **kwargs):
    app = kwargs.pop("app", None)
    if app is not None and "transport" not in kwargs:
        kwargs["transport"] = ASGITransport(app=app)
    original_init(self, *args, **kwargs)

httpx.Client.__init__ = patched_init
