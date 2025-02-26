from iotdb.ainode.model.TimerXL.models import timer_xl


class ExpBasic(object):
    def __init__(self, args):
        self.args = args
        self.model_dict = {
            # 'timer': timer,
            'timer_xl': timer_xl,
        }
        self.model = self._build_model()

    def _build_model(self):
        raise NotImplementedError

    def _get_data(self):
        pass

    def vali(self):
        pass

    def train(self):
        pass

    def test(self):
        pass
