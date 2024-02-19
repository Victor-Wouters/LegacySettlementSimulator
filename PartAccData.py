import csv
import ParticipantModule


def read_csv_and_create_participants(filename):

    participants_directory = {}

    with open(filename, newline='') as csvfile:

        reader = csv.reader(csvfile, delimiter=';')

        next(reader)  # Skip the header row
        for row in reader:
            part_id, account_id, balance, credit_limit = row
            if part_id not in participants_directory:
                participants_directory[part_id] = ParticipantModule.Participant(part_id)
            participants_directory[part_id].add_account(account_id, float(balance), float(credit_limit))

    #return list(participants.values())
    return participants_directory


#participants = read_csv_and_create_participants('data\PART.csv')

#Search examples:

#print(participants.get('1').get_account('1').get_balance())

#for i in participants.values():
#    print(i.get_part_id())
#    print(i.get_account('1').get_balance())
#    print(i.get_account('3').get_credit_limit())