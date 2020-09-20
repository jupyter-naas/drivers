from random import randint

class Template:
    def help(self):
        print('.help() => Get help on what you can do with this driver\n')
        print('.hello(name) => Get the hello string, optionally you can pass name, otherwise by default it\'s bob\n')
        print('.bye(name) => Get the bye string, who say a random number of bye (between 1-10). optionally you can pass name, otherwise by default it\'s bob\n')

    def hello(self, name='Bob'):
        res = 'Hello ' + name
        return res

    def bye(self, name='Bob'):
        bye_num = randint(0, 10)
        res = 'Bye ' + ((',bye ' * (bye_num - 1)) if bye_num > 1 else '') + name
        return res

# In complement of you driver code, use the template_driver.ipynb to create a notebook exemple in shared/driver folder with exaustives exemples

# tmp = Template()
# print(tmp.hello())
# print(tmp.bye())