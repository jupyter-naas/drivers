import copy


class Utils:
    def replace(self, text, data):
        text_copy = copy(text)
        for key in data.keys():
            text_copy = text_copy.replace(key, data[key])
        return
