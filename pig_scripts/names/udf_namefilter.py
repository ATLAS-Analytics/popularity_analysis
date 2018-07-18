#udf to filter out robot usernames
@outputSchema("robot:boolean")
def isRobot(user):
    #all the usernames including CN=Robot
    #if 'Robot'in user:
    if 'gangarbt' in user:
        return True
    #all other usernames that I think are automated
    #elif user in ('atlas-dpd-production', 'atlas-dpd-prodcution', 'phys-gener', 'phys-higgs', 'pilot', 'atlascdb', 'gangarbt'):
    #    return True
    else:
        return False

@outputSchema("user:chararray")
def getUser(user):
    if 'CN' in user:
        
        #split the username
        parts = user.split("/")

        #find the correct section and return it
        for i in range(len(parts)):
            if 'CN' in parts[i]:
                if '@' in parts[i]:
                    words = parts[i].split(' ')[:-1]
                    parts = ' '.join(words)
                    out = parts[3:]
                else:
                    out = parts[i][3:]
                break
        return out
    
    else:
        return user

#testing
if __name__ == '__main__':
    print getUser('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=abell/CN=763097/CN=Andrew Stuart Bell/CN=1465264239')
    print getUser('/C=DE/O=GermanGrid/OU=MPPMU/CN=Nicolas Maximilian Koehler/CN=947318765')
    print getUser('/DC=org/DC=terena/DC=tcs/C=NL/O=Nikhef/CN=Brian Moser brainm@nikhef.nl/CN=1439478909')
