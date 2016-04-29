from application.db import server
from application.machine.transform import RollingKFold
from sklearn.cross_validation import KFold

test0 = KFold(10)
test1 = RollingKFold(10)

print [item for item in test0]
print [item for item in test1]
