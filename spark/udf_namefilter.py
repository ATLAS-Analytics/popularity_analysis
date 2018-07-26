import re
from datetime import datetime

#udf to filter out ganga accesses
def isGanga(user):
    if 'ganga' in user:
        return True
    else:
        return False

#udf to filter out robot usernames
def isRobot(user):
    #all the usernames including CN=Robot
    #if 'Robot'in user:
    if 'Robot' in user:
        return True
    #all other usernames that I think are automated
    elif user in ('atlas-dpd-production', 'atlas-dpd-prodcution', 'phys-gener', 'phys-higgs', 'pilot', 'atlascdb', 'gangarbt'):
        return True
    else:
        return False

#function to get username or fullname from long username
def getUser(user):
    if '/CN=' in user:
        
        #split the username
        parts = user.split("/")

        #find the correct section and return it
        for i in range(len(parts)):
            #first incidence of CN is username or name of persion
            if 'CN=' in parts[i]:
                #if it contains email address strip that out
                if '@' in parts[i]:
                    words = parts[i].split(' ')[:-1]
                else:
                    words = parts[i].split(' ')
                    #if it contains three letters and three numbers take that out
                    if re.match('^[a-z]{3}-[\d]{3}', words[-1]):
                        print words[-1]
                        words = words[:-1]
                parts = ' '.join(words)
                out = parts[3:]
                break
        
        return out.lower() #return name in lowercase
    
    else:
        return user

def getTime(timestamp):
    timeList = timestamp.split("-")
    d = datetime(int(timeList[0]), int(timeList[1]), int(timeList[2]))
    epochtime = (d-datetime(1970,1,1)).total_seconds()
    return epochtime * 1000
