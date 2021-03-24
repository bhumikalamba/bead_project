from model import User

from typing import Any, Union
from neomodel import db, Q
#
# user = User(id_str=1234,screen_name='Tester').save()
# print(user)
#
# user_2 = User(id_str=124,screen_name='TesterFollower',follows=user).save()
#
# print(user_2)

nodes = User.nodes.all()

print(nodes)