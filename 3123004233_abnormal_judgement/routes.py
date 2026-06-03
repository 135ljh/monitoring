import operations


def refresh(cfg):
    for item in list:
        item.recv_topic = cfg.SUB_TOPIC + "/" + item.operation
        item.send_topic = cfg.PUB_TOPIC + "/" + item.operation


class Route(object):
    def __init__(self, desc, operation):
        self.desc = desc
        self.operation = operation
        self.func = getattr(operations, operation)


list = [
    Route(desc="""""",
          operation="start_judge",
          )

]
