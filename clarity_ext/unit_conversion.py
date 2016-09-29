import math


class UnitConversion(object):
    NANO = -9
    PICO = -12

    MAPPING = {
        NANO: "n",
        PICO: "p"
    }

    def __init__(self):
        pass

    def convert(self, value, unit_from, unit_to):
        if unit_from == unit_to:
            return value
        factor = math.pow(10, unit_from - unit_to)
        ret = value * factor
        return ret

    def unit_to_string(self, unit):
        return self.MAPPING[unit]

