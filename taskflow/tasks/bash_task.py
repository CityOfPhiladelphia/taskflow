import os
import signal
import logging
import re
import sys
import shutil
import time
from subprocess import Popen, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from threading import Thread
from contextlib import contextmanager
from tempfile import mkdtemp

from smart_open import smart_open
import boto
import boto3

from taskflow import Task

def pipe_stream(stream1, stream2):
    def stream_helper(stream1, stream2):
        for line in iter(stream1.readline, b''):
            stream2.write(line)

    t = Thread(target=stream_helper, args=(stream1, stream2))
    t.daemon = True
    t.start()

    return t

@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
        try:
            shutil.rmtree(name)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

def replace_environment_variables(input_str):
    env_vars = re.findall(r'"?\$([A-Z0-9_-]+)"?', input_str)

    out_str = input_str
    for env_var in env_vars:
        value = os.getenv(env_var, '')
        out_str = re.sub('"?\$' + env_var + '"?', value, out_str)
    return out_str

class BashTask(Task):
    def get_command(self):
        return self.params['command']

    def execute(self, task_instance):
        logger = logging.getLogger(self.name)
        bash_command = self.get_command()
        logger.info('Temporary directory root location: %s', gettempdir())
        with TemporaryDirectory(prefix='taskflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=str(task_instance.id)) as f:
                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logger.info('Temporary script location: %s', script_location)
                logger.info('Running command: %s', bash_command)

                inputpath = None
                if 'input_file' in task_instance.params and task_instance.params['input_file'] != None:
                    inputpath = replace_environment_variables(task_instance.params['input_file'])
                elif 'input_file' in self.params and self.params['input_file'] != None:
                    inputpath = replace_environment_variables(self.params['input_file'])

                input_file = None
                if inputpath:
                    logger.info('Streaming to STDIN from: %s', inputpath)
                    input_file = smart_open(inputpath, mode='rb')

                outpath = None
                if 'output_file' in task_instance.params and task_instance.params['output_file'] != None:
                    outpath = replace_environment_variables(task_instance.params['output_file'])
                elif 'output_file' in self.params and self.params['output_file'] != None:
                    outpath = replace_environment_variables(self.params['output_file'])

                output_file = None
                if outpath:
                    logger.info('Streaming STDOUT to: %s', outpath)
                    output_file = smart_open(outpath, mode='wb')

                ON_POSIX = 'posix' in sys.builtin_module_names

                sp = Popen(
                    ['bash', fname],
                    stdin=PIPE if input_file else None,
                    stdout=PIPE if output_file else None,
                    stderr=PIPE,
                    cwd=tmp_dir,
                    preexec_fn=os.setsid,
                    bufsize=1,
                    close_fds=ON_POSIX)

                self.sp = sp

                if input_file:
                    input_thread = pipe_stream(input_file, sp.stdin)

                if output_file:
                    output_thread = pipe_stream(sp.stdout, output_file)

                for line in iter(sp.stderr.readline, b''):
                    logger.info(line)

                sp.wait()

                if input_thread:
                    input_thread.join(timeout=5)

                if output_thread:
                    output_thread.join(timeout=5)

                if input_file:
                    input_file.close()

                if output_file:
                    logger.info('Closing STDOUT file')
                    start = time.time()
                    output_file.close()
                    logger.info('STDOUT file written - {}s'.format(time.time() - start))

                logger.info('Command exited with return code %s', sp.returncode)

                if sp.returncode:
                    raise Exception('Bash command failed')

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
