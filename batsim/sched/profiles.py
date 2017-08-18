from abc import ABCMeta, abstractmethod


class Profile(metaclass=ABCMeta):

    @abstractmethod
    def to_dict(self):
        pass

    def __call__(self):
        return self.to_dict()


class Profiles(metaclass=ABCMeta):

    class Delay(Profile):

        def __init__(self, time):
            self.time = time

        def to_dict(self):
            return {
                "type": "delay",
                "delay": self.time
            }
