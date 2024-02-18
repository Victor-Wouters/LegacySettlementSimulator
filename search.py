import PartAccData

for i in PartAccData.participants.values():
    print(i.get_part_id())
    print(i.get_account('1').get_balance())
    print(i.get_account('3').get_credit_limit())