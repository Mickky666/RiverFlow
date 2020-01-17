import inspect


class _PipeMeta(type):

    def __new__(typ, *args, **kwargs):
        print("New _PipeMeta")
        print(inspect.getargspec(args[2]['run']))
        import ipdb; ipdb.set_trace()

        return super(_PipeMeta, typ).__new__(typ, *args, **kwargs)

    def __init__(cls, *args, **kwargs):
        print("Init _PipeMeta")
        import ipdb; ipdb.set_trace()
        super(_PipeMeta, cls).__init__(*args, **kwargs)


class Pipe(object):
    __metaclass__ = _PipeMeta

    def __new__(cls, *args, **kwargs):
        print("New Pipe")
        import ipdb; ipdb.set_trace()
        return super(Pipe, cls).__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        print("New Pipe")
        import ipdb; ipdb.set_trace()
        super(Pipe, self).__init__(*args, **kwargs)

    def run(self, flow_pool, mode):
        pass


class pipe(Pipe):

    def run(self, flow_pool, mode, ahaha):
        pass

aaa = pipe("5")
import ipdb; ipdb.set_trace()