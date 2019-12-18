class _PipeMeta(type):

    def __new__(typ, *args, **kwargs):
        import ipdb; ipdb.set_trace()
        return super(_PipeMeta, typ).__new__(typ, *args, **kwargs)


class Pipe(object):
    __metaclass__ = _PipeMeta

    def __new__(cls, *args, **kwargs):
        import ipdb; ipdb.set_trace()
        return super(Pipe, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        import ipdb; ipdb.set_trace()


aaa = Pipe("5")
import ipdb; ipdb.set_trace()