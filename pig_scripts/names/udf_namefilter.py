import re
#udf to filter out ganga accesses
@outputSchema("ganga:boolean")
def isGanga(user):
    if 'ganga' in user:
        return True
    else:
        return False


#udf to filter out robot usernames
@outputSchema("robot:boolean")
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

@outputSchema("user:chararray")
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
        
#testing
if __name__ == '__main__':
    print getUser('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=abell/CN=763097/CN=Andrew Stuart Bell/CN=1465264239')
    print getUser('/C=DE/O=GermanGrid/OU=MPPMU/CN=Nicolas Maximilian Koehler/CN=947318765')
    print getUser('/DC=org/DC=terena/DC=tcs/C=NL/O=Nikhef/CN=Brian Moser brainm@nikhef.nl/CN=1439478909')
    print getUser('/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Andrew Stuart Bell rtp-042/CN=1465264239')
    print getUser('/C=CN/O=HEP/O=IHEP/OU=PHYS/CN=Javier Llorente Merino/CN=2131881940')
