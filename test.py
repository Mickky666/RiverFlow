from pgraph import Pipe, Config


class PipeA(Pipe):

    def run(self, flow_pool, config, mode):
        print("pipe A" + "=" * 100)
        flow_pool.write_flow("output_a", "aaa")
        config.get_config("a")


class PipeB(Pipe):

    def run(self, flow_pool, config, mode):
        print("pipe B" + "=" * 100)
        flow_pool.read_flow("output_a")
        flow_pool.write_flow("output_b", "bbb")
        config.get_config("b")


a = PipeA()
b = PipeB()
pipeline = a >> b
config = Config({"a": 1, "b": 2.})

pipeline.run(config)



