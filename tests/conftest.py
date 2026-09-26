"""
Shared pytest setup. Runs before test modules are imported.
"""
import os
import tempfile

# comms_api creates MEDIA_DIR (default /app/media) at import time; CI runners
# can't write to /app, so point uploads at a throwaway directory for tests.
os.environ["MEDIA_DIR"] = tempfile.mkdtemp(prefix="imm-media-")
