# Sanitize/deduplicate field values to be counted.

import re
from datetime import date, timedelta

# Regular expressions.     

# Add suffix to name separated by a space, if suffix is non-empty.
def add_suffix(name, suffix):
    if len(suffix) > 0:
        return name + ' ' + suffix
    return name

# Regular expressions for checking validity and formatting values. 
matches = dict(
    valid_os = re.compile('(' +
        # Standard string.
        '^(\d\.){3}\d([.\-]prerelease)?$' + '|' +
        # Tarako/India devices. 
        '^(ind|intex)_' +
        ')', re.I)    
)

# Substitution patterns for formatting field values.
subs = dict(
    os = [{
        'regex': re.compile('[.\-]prerelease$', re.I),
        'repl': ' (pre-release)'
    },{
        'regex': re.compile(
            '^(?P<num>[1-9]\.[0-9](\.[1-9]){0,2})(\.0){0,2}', re.I),
        'repl': '\g<num>'
    }],
    
    device = [{
        # One Touch Fire. 
        'regex': re.compile(
            '^.*one\s*touch.*fire\s*(?P<suffix>[ce]?)(?:\s+\S*)?$', re.I),
        'repl': lambda match: add_suffix('One Touch Fire', 
            match.group('suffix').upper())
    },{
       # Open.
        'regex': re.compile(
            '^.*open\s*(?P<suffix>[2c])(?:\\s+\\S*)?$', re.I),
        'repl': lambda match: 'ZTE Open ' + match.group('suffix').upper()
    },{
        # Flame.
        'regex': re.compile('^.*flame.*$', re.I),
        'repl': 'Flame'
    },{ 
        # Geeksphone.
        'regex': re.compile('^.*(keon|peak).*$', re.I),
        'repl': lambda match: 'Geeksphone ' + match.group(1).capitalize()
    },{
        # Emulators/dev devices
        'regex': re.compile('^.*(android|aosp).*$', re.I),
        'repl': 'Emulator/Android'
    },{
        # Tarako - Cloud FX.
        'regex': re.compile('^.*clou.d\\s*fx.*$', re.I),
        'repl': 'Cloud FX (Tarako)'
    },{
        # Tarako - Spice.
        'regex': re.compile('^.*spice\\s*mifx1.*$', re.I),
        'repl': 'Spice MIFX1 (Tarako)'
    }],
    
    operator = [{
        'regex': re.compile('^A1.*$', re.I),
        'repl': 'A1'
    },{
        'regex': re.compile('^Aircel.*$', re.I),
        'repl': 'Aircel'
    },{
        'regex': re.compile('^Airtel.*$', re.I),
        'repl': 'Airtel'
    },{
        'regex': re.compile('^AIS.*$', re.I),
        'repl': 'AIS'
    },{
        'regex': re.compile('^Alltel.*$', re.I),
        'repl': 'Alltel'
    },{
        'regex': re.compile('^AT&T.*$', re.I),
        'repl': 'AT&T'
    },{
        'regex': re.compile('^B-Mobile.*$', re.I),
        'repl': 'B-Mobile'
    },{
        'regex': re.compile('^Base.*$', re.I),
        'repl': 'Base'
    },{
        'regex': re.compile('^Batelco.*$', re.I),
        'repl': 'Batelco'
    },{
        'regex': re.compile('^Bell.*$', re.I),
        'repl': 'Bell'
    },{
        'regex': re.compile('^Bite.*$', re.I),
        'repl': 'Bite'
    },{
        'regex': re.compile('^blau.*$', re.I),
        'repl': 'blau'
    },{
        'regex': re.compile('^Bob.*$', re.I),
        'repl': 'Bob'
    },{
        'regex': re.compile('^Bouygues.*$', re.I),
        'repl': 'Bouygues'
    },{
        'regex': re.compile('^Breeze.*$', re.I),
        'repl': 'Breeze'
    },{
        'regex': re.compile('^CCT.*$', re.I),
        'repl': 'CCT'
    },{
        'regex': re.compile('^Cellular One.*$', re.I),
        'repl': 'Cellular One'
    },{
        'regex': re.compile('^Claro.*$', re.I),
        'repl': 'Claro'
    },{
        'regex': re.compile('^Cloud9.*$', re.I),
        'repl': 'Cloud9'
    },{
        'regex': re.compile('^Comcel.*$', re.I),
        'repl': 'Comcel'
    },{
        'regex': re.compile('^Congstar.*$', re.I),
        'repl': 'Congstar'
    },{
        'regex': re.compile('^Corr.*$', re.I),
        'repl': 'Corr'
    },{
        'regex': re.compile('^CTBC.*$', re.I),
        'repl': 'CTBC'
    },{
        'regex': re.compile('^delight.*$', re.I),
        'repl': 'delight'
    },{
        'regex': re.compile('^Digicel.*$', re.I),
        'repl': 'Digicel'
    },{
        'regex': re.compile('^Digitel.*$', re.I),
        'repl': 'Digitel'
    },{
        'regex': re.compile('^Digital.*$', re.I),
        'repl': 'Digital'
    },{
        'regex': re.compile('^disco.*$', re.I),
        'repl': 'disco'
    },{
        'regex': re.compile('^Djuice.*$', re.I),
        'repl': 'Djuice'
    },{
        'regex': re.compile('^DNA.*$', re.I),
        'repl': 'DNA'
    },{
        'regex': re.compile('^Dolphin.*$', re.I),
        'repl': 'Dolphin'
    },{
        'regex': re.compile('^DTAC.*$', re.I),
        'repl': 'DTAC'
    },{
        'regex': re.compile('^E-Plus.*$', re.I),
        'repl': 'E-Plus'
    },{
        'regex': re.compile('^Econet.*$', re.I),
        'repl': 'Econet'
    },{
        'regex': re.compile('^eMobile.*$', re.I),
        'repl': 'eMobile'
    },{
        'regex': re.compile('^Emtel.*$', re.I),
        'repl': 'Emtel'
    },{
        'regex': re.compile('^Entel.*$', re.I),
        'repl': 'Entel'
    },{
        'regex': re.compile('^Etisalat.*$', re.I),
        'repl': 'Etisalat'
    },{
        'regex': re.compile('^Euskatel.*$', re.I),
        'repl': 'Euskatel'
    },{
        'regex': re.compile('^Farmers.*$', re.I),
        'repl': 'Farmers'
    },{
        'regex': re.compile('^Fastweb.*$', re.I),
        'repl': 'Fastweb'
    },{
        'regex': re.compile('^Fonex.*$', re.I),
        'repl': 'Fonex'
    },{
        'regex': re.compile('^Free.*$', re.I),
        'repl': 'Free'
    },{
        'regex': re.compile('^Gemalto.*$', re.I),
        'repl': 'Gemalto'
    },{
        'regex': re.compile('^Globalstar.*$', re.I),
        'repl': 'Globalstar'
    },{
        'regex': re.compile('^Globe.*$', re.I),
        'repl': 'Globe'
    },{
        'regex': re.compile('^GLOBUL.*$', re.I),
        'repl': 'GLOBUL'
    },{
        'regex': re.compile('^Golan.*$', re.I),
        'repl': 'Golan'
    },{
        'regex': re.compile('^Golden Telecom.*$', re.I),
        'repl': 'Golden Telecom'
    },{
        'regex': re.compile('^Hello.*$', re.I),
        'repl': 'Hello'
    },{
        'regex': re.compile('^Highland.*$', re.I),
        'repl': 'Highland'
    },{
        'regex': re.compile('^Hits.*$', re.I),
        'repl': 'Hits'
    },{
        'regex': re.compile('^Hormuud.*$', re.I),
        'repl': 'Hormuud'
    },{
        'regex': re.compile('^HT.*$', re.I),
        'repl': 'HT'
    },{
        'regex': re.compile('^ICE.*$', re.I),
        'repl': 'ICE'
    },{
        'regex': re.compile('^Idea.*$', re.I),
        'repl': 'Idea'
    },{
        'regex': re.compile('^Indigo.*$', re.I),
        'repl': 'Indigo'
    },{
        'regex': re.compile('^Indosat.*$', re.I),
        'repl': 'Indosat'
    },{
        'regex': re.compile('^Jawwal.*$', re.I),
        'repl': 'Jawwal'
    },{
        'regex': re.compile('^Jazztel.*$', re.I),
        'repl': 'Jazztel'
    },{
        'regex': re.compile('^KTF.*$', re.I),
        'repl': 'KTF'
    },{
        'regex': re.compile('^Libertis.*$', re.I),
        'repl': 'Libertis'
    },{
        'regex': re.compile('^Maroc Telecom.*$', re.I),
        'repl': 'Maroc Telecom'
    },{
        'regex': re.compile('^MIO.*$', re.I),
        'repl': 'MIO'
    },{
        'regex': re.compile('^Mobilis.*$', re.I),
        'repl': 'Mobilis'
    },{
        'regex': re.compile('^mobilR.*$', re.I),
        'repl': 'mobilR'
    },{
        'regex': re.compile('^mobily.*$', re.I),
        'repl': 'mobily'
    },{
        'regex': re.compile('^Mobistar.*$', re.I),
        'repl': 'Mobistar'
    },{
        'regex': re.compile('^Moov.*$', re.I),
        'repl': 'Moov'
    },{
        'regex': re.compile('^Movilnet.*$', re.I),
        'repl': 'Movilnet'
    },{
        'regex': re.compile('^Namaste.*$', re.I),
        'repl': 'Namaste'
    },{
        'regex': re.compile('^Nawras.*$', re.I),
        'repl': 'Nawras'
    },{
        'regex': re.compile('^NEP.*$', re.I),
        'repl': 'NEP'
    },{
        'regex': re.compile('^Netz.*$', re.I),
        'repl': 'Netz'
    },{
        'regex': re.compile('^Nextel.*$', re.I),
        'repl': 'Nextel'
    },{
        'regex': re.compile('^Nitz.*$', re.I),
        'repl': 'Nitz'
    },{
        'regex': re.compile('^O2.*$', re.I),
        'repl': 'O2'
    },{
        'regex': re.compile('^olleh.*$', re.I),
        'repl': 'olleh'
    },{
        'regex': re.compile('^One.Tel.*$', re.I),
        'repl': 'One.Tel'
    },{
        'regex': re.compile('^OnePhone.*$', re.I),
        'repl': 'OnePhone'
    },{
        'regex': re.compile('^Orange.*$', re.I),
        'repl': 'Orange'
    },{
        'regex': re.compile('^Outremer.*$', re.I),
        'repl': 'Outremer'
    },{
        'regex': re.compile('^OY.*$', re.I),
        'repl': 'OY'
    },{
        'regex': re.compile('^Play.*$', re.I),
        'repl': 'Play'
    },{
        'regex': re.compile('^Plus.*$', re.I),
        'repl': 'Plus'
    },{
        'regex': re.compile('^Poka Lambro.*$', re.I),
        'repl': 'Poka Lambro'
    },{
        'regex': re.compile('^Polska Telefonia.*$', re.I),
        'repl': 'Polska Telefonia'
    },{
        'regex': re.compile('^Reliance.*$', re.I),
        'repl': 'Reliance'
    },{
        'regex': re.compile('^Rogers.*$', re.I),
        'repl': 'Rogers'
    },{
        'regex': re.compile('^Rwandatel.*$', re.I),
        'repl': 'Rwandatel'
    },{
        'regex': re.compile('^Scarlet.*$', re.I),
        'repl': 'Scarlet'
    },{
        'regex': re.compile('^SERCOM.*$', re.I),
        'repl': 'SERCOM'
    },{
        'regex': re.compile('^SFR.*$', re.I),
        'repl': 'SFR'
    },{
        'regex': re.compile('^Simyo.*$', re.I),
        'repl': 'Simyo'
    },{
        'regex': re.compile('^SingTel.*$', re.I),
        'repl': 'SingTel'
    },{
        'regex': re.compile('^SKT.*$', re.I),
        'repl': 'SKT'
    },{
        'regex': re.compile('^SmarTone.*$', re.I),
        'repl': 'SmarTone'
    },{
        'regex': re.compile('^Smile.*$', re.I),
        'repl': 'Smile'
    },{
        'regex': re.compile('^Softbank.*$', re.I),
        'repl': 'Softbank'
    },{
        'regex': re.compile('^Southern Communications.*$', re.I),
        'repl': 'Southern Communications'
    },{
        'regex': re.compile('^Spacetel.*$', re.I),
        'repl': 'Spacetel'
    },{
        'regex': re.compile('^Tango.*$', re.I),
        'repl': 'Tango'
    },{
        'regex': re.compile('^Telcel.*$', re.I),
        'repl': 'Telcel'
    },{
        'regex': re.compile('^Telenor.*$', re.I),
        'repl': 'Telenor'
    },{
        'regex': re.compile('^Teletalk.*$', re.I),
        'repl': 'Teletalk'
    },{
        'regex': re.compile('^Tele.ring.*$', re.I),
        'repl': 'Tele.ring'
    },{
        'regex': re.compile('^Telma.*$', re.I),
        'repl': 'Telma'
    },{
        'regex': re.compile('^Telstra.*$', re.I),
        'repl': 'Telstra'
    },{
        'regex': re.compile('^Telus.*$', re.I),
        'repl': 'Telus'
    },{
        'regex': re.compile('^Tesco.*$', re.I),
        'repl': 'Tesco'
    },{
        'regex': re.compile('^Test.*$', re.I),
        'repl': 'Test'
    },{
        'regex': re.compile('^Thinta.*$', re.I),
        'repl': 'Thinta'
    },{
        'regex': re.compile('^Thuraya.*$', re.I),
        'repl': 'Thuraya'
    },{
        'regex': re.compile('^Tigo.*$', re.I),
        'repl': 'Tigo'
    },{
        'regex': re.compile('^TMA.*$', re.I),
        'repl': 'TMA'
    },{
        'regex': re.compile('^True.*$', re.I),
        'repl': 'True'
    },{
        'regex': re.compile('^Tuenti.*$', re.I),
        'repl': 'Tuenti'
    },{
        'regex': re.compile('^Unicom.*$', re.I),
        'repl': 'Unicom'
    },{
        'regex': re.compile('^Uninor.*$', re.I),
        'repl': 'Uninor'
    },{
        'regex': re.compile('^UTS.*$', re.I),
        'repl': 'UTS'
    },{
        'regex': re.compile('^Vectone.*$', re.I),
        'repl': 'Vectone'
    },{
        'regex': re.compile('^Velcom.*$', re.I),
        'repl': 'Velcom'
    },{
        'regex': re.compile('^Viettel.*$', re.I),
        'repl': 'Viettel'
    },{
        'regex': re.compile('^VIP.*$', re.I),
        'repl': 'VIP'
    },{
        'regex': re.compile('^Virgin.*$', re.I),
        'repl': 'Virgin'
    },{
        'regex': re.compile('^Viva.*$', re.I),
        'repl': 'Viva'
    },{
        'regex': re.compile('^Vivo.*$', re.I),
        'repl': 'Vivo'
    },{
        'regex': re.compile('^VoiceStream.*$', re.I),
        'repl': 'VoiceStream'
    },{
        'regex': re.compile('^VTR.*$', re.I),
        'repl': 'VTR'
    },{
        'regex': re.compile('^Warid.*$', re.I),
        'repl': 'Warid'
    },{
        'regex': re.compile('^Wataniya.*$', re.I),
        'repl': 'Wataniya'
    },{
        'regex': re.compile('^Wind.*$', re.I),
        'repl': 'Wind'
    },{
        'regex': re.compile('^XL.*$', re.I),
        'repl': 'XL'
    },{
        'regex': re.compile('^Yesss.*$', re.I),
        'repl': 'Yesss'
    },{
        'regex': re.compile('^Yoigo.*$', re.I),
        'repl': 'Yoigo'
    },{
        'regex': re.compile('^Zain.*$', re.I),
        'repl': 'Zain'
    },{
        'regex': re.compile('^!dea(\s.+)?$', re.I),
        'repl': 'Idea'
    },{
        'regex': re.compile('^3[^\w].+$', re.I),
        'repl': '3'
    },{
        'regex': re.compile('^bee\s*line(\s.+)?$', re.I),
        'repl': 'Beeline'
    },{
        'regex': re.compile('^bh\s*mobile(\s.+)?$', re.I),
        'repl': 'BH Mobile'
    },{
        'regex': re.compile('^(.+\s)?bsnl(\s.+)?$', re.I),
        'repl': 'BSNL'
    },{
        'regex': re.compile('^cab(le|el) (&|and) wireless.*$', re.I),
        'repl': 'Cable & Wireless'
    },{
        'regex': re.compile('celcom', re.I),
        'repl': 'Cellcom'
    },{
        'regex': re.compile(
            '^(?:.+\s)?china.*\s(?P<suffix>mobile|telecom|unicom)(\s.+)?$', re.I),
        'repl': lambda match: 'China ' + match.group('suffix').capitalize()
    },{
        'regex': re.compile('^(chn-)?(unicom|cu[^\w]*(cc|gsm)).*$', re.I),
        'repl': 'China Unicom'
    },{
        'regex': re.compile('^CMCC$', re.I),
        'repl': 'China Mobile'
    },{
        'regex': re.compile('^(chungh?wa.*|CHT)$', re.I),
        'repl': 'Chunghwa'
    },{
        'regex': re.compile('^.*cingular.*$', re.I),
        'repl': 'Cingular'
    },{
        'regex': re.compile('^(.+\s)?cosmote(\s.+)?$', re.I),
        'repl': 'Cosmote'
    },{
        'regex': re.compile('^da?tatel(\s.+)?$', re.I),
        'repl': 'Datatel'
    },{
        'regex': re.compile('^diall?og$', re.I),
        'repl': 'Dialog'
    },{
        'regex': re.compile('^digi([^\w]+.*)?$', re.I),
        'repl': 'Digi'
    },{
        'regex': re.compile('^(.+\s)?docomo(\s.+)?$', re.I),
        'repl': 'Docomo'
    },{
        'regex': re.compile('^esto es el.+$', re.I), 
        'repl': 'Unknown'
    },{
        'regex': re.compile('^glo(\s.+)?$', re.I),
        'repl': 'Glo'
    },{
        'regex': re.compile('^gramee?nphone$', re.I),
        'repl': 'GrameenPhone'
    },{
        'regex': re.compile('^guin.tel.*$', re.I),
        'repl': 'Guinetel'
    },{
        'regex': re.compile('^life(\s.+)?$', re.I),
        'repl': 'life:)'
    },{
        'regex': re.compile('^lime(\s.+)?$', re.I),
        'repl': 'Lime'
    },{
        'regex': re.compile('^lyca.*$', re.I),
        'repl': 'Lyca Mobile'
    },{
        'regex': re.compile('^m[:-]?tel(\s.+)?$', re.I),
        'repl': 'M-Tel'
    },{
        'regex': re.compile('^medion\s*mobile(\s.+)?', re.I),
        'repl': 'Medion'
    },{
        'regex': re.compile('^mobil?com([^\w].+)?$', re.I),
        'repl': 'Mobilcom'
    },{
        'regex': re.compile('^mobil?tel(\s.+)?$', re.I),
        'repl': 'Mobitel'
    },{
        'regex': re.compile('^(.+\s)?movie?star(\s.+)?$', re.I),
        'repl': 'Movistar'
    },{
        'regex': re.compile('^mt:?(?P<suffix>[cns])([^\w].*)?$', re.I),
        'repl': lambda match: 'MT' + match.group('suffix').upper()
    },{
        'regex': re.compile('^mudio', re.I),
        'repl': 'Mundio'
    },{
        'regex': re.compile('^oi(\s.+)?$', re.I),
        'repl': 'Oi'
    },{
        'regex': re.compile('^proxi(mus)?(\s.+)?$', re.I),
        'repl': 'Proximus'
    },{
        'regex': re.compile('^Sask\s?[Ttel].*$', re.I),
        'repl': 'SaskTel'
    },{
        'regex': re.compile('^smarts?(\s.+)?$', re.I),
        'repl': 'Smart'
    },{
        'regex': re.compile('^s\s+tel.*$', re.I),
        'repl': 'S Tel'
    },{
        'regex': re.compile('^sun(\s.+)?$', re.I),
        'repl': 'Sun'
    },{
        'regex': re.compile('^t\s*-\s*mobile.*$', re.I),
        'repl': 'T-Mobile'
    },{
        'regex': re.compile('^.*tele?\s*2.*$', re.I),
        'repl': 'Tele2'
    },{
        'regex': re.compile('^tel\w+\scel$', re.I),
        'repl': 'Telecel'
    },{
        'regex': re.compile('^telekom\.de(\s.+)?$', re.I),
        'repl': 'T-Mobile'
    },{
        'regex': re.compile('^telekom(\.|\s)hu(\s.+)?$', re.I),
        'repl': 'T-Mobile'
    },{
        'regex': re.compile('^tm([^\w].+)?$', re.I),
        'repl': 'TM'
    },{
        'regex': re.compile('^tw\s*m(obile)?(\s.+)?$', re.I),
        'repl': 'Taiwan Mobile'
    },{
        'regex': re.compile('^.*verizon.*$', re.I),
        'repl': 'Verizon'
    },{
        'regex': re.compile('^vid.otron.*$', re.I),
        'repl': 'Videotron'
    },{
        'regex': re.compile('^vip([^\w].*)?$', re.I),
        'repl': 'VIP'
    },{
        'regex': re.compile('^voda.*$', re.I),
        'repl': 'Vodafone'
    },{
        'regex': re.compile('^W1(\s.+)?$', re.I),
        'repl': 'WirelessOne'
    },{
        'regex': re.compile('^Wikes Cellular$', re.I), 
        'repl': 'Wilkes Cellular'
    }]
)

# Date range for ping dates to be considered valid.
valid_dates = {
    'earliest': date(2014, 4, 1),
    # Latest: a few days before today's date. 
    'latest': date.today() - timedelta(1)
}



#--------------------

# Processing for each individual field. 

# Convert the pingTime timestamp to a date.
# If an invalid condition occurs, throws ValueError with a custom message.
def get_ping_date(val):
    if val is None:
        raise ValueError('no ping time')
    
    try: 
        # pingTime is millisecond-resolution timestamp.
        val = int(val) / 1000
        pingdate = date.fromtimestamp(val)
    except Exception:
        raise ValueError('invalid ping time')
    
    # Enforce date range.
    if pingdate < valid_dates['earliest'] or pingdate > valid_dates['latest']:
        raise ValueError('outside date range')
    
    return pingdate


# Parse OS version. 
# If an invalid condition occurs, throws ValueError with a custom message.
# Do not include "Other" field for OS - 
# drop records with non-matching values instead.
def get_os_version(val):    
    if val is None:
        raise ValueError('no os version')
    os = unicode(val)
  
    # Check OS against expected format. 
    if matches['valid_os'].match(os) is None:
        raise ValueError('invalid os version')
    
    # Reformat to be more readable. 
    for s in subs['os']:
        os = s['regex'].sub(s['repl'], os, count = 1)
        
    return os


# Format device name. 
# Only record distinct counts for certain recognized device names.
# Pass recognized_list as a tuple. 
def get_device_name(val, recognized_list):
    if val is None:
        return 'Unknown'
    device = unicode(val)
    
    # Make formatting consistent to avoid duplication.
    # Apply replacement regexes.
    for s in subs['device']:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit loop. 
        formatted, n = s['regex'].subn(s['repl'], device, count = 1)
        if n > 0:
            device = formatted
            break
    
    # Don't keep distinct name if does not start with recognized prefix.
    if not device.startswith(recognized_list): 
        return 'Other'
    
    return device


# Look up country name from 2-letter code. 
# Only record counts for recognized countries. 
# Pass recognized_list as a set.
def get_country(val, recognized_list, country_codes):
    if val is None:
        return 'Unknown'
    geo = unicode(val)
    
    # Look up country name. 
    if geo not in country_codes: 
        return 'Unknown'
    
    geo = country_codes[geo]['name']
    # Don't keep distinct name if not in recognized list. 
    if geo not in recognized_list: 
        return 'Other'
        
    return geo


# Look up mobile operator using mobile codes.
def lookup_operator_from_codes(fields, mobile_codes):
    if 'mcc' not in fields or 'mnc' not in fields:
        # Missing codes. 
        return None
    
    if fields['mcc'] not in mobile_codes:
        # Country code is not recognized in lookup table.
        return None
    
    return mobile_codes[fields['mcc']]['operators'].get(fields['mnc'])

    
# Look up mobile operator from field in payload.
def lookup_operator_from_field(fields, key):
    operator = fields.get(key)
    if operator is None:
        return None
        
    operator = str(operator).strip()
    if len(operator) == 0:
        return None
    
    return operator
    
    
# Logic to look up operator name from payload.
# Try looking up operator from SIM/ICC codes, if available. 
# If that fails, try using SIM SPN. 
# If no SIM is present, look up operator from network codes.
# If that fails, try reading network operator name field. 
# If none of these are present, operator is 'Unknown'.
def lookup_operator(icc_fields, network_fields, mobile_codes):
    if icc_fields is not None:
        # SIM is present. 
        operator = lookup_operator_from_codes(icc_fields, mobile_codes)
        if operator is not None:
            return operator
        
        # At this point, we were not able to resolve the operator 
        # from the codes.
        # Try the name string instead.
        operator = lookup_operator_from_field(icc_fields, 'spn')
        if operator is not None:
            return operator
    
    # Lookup using SIM card info failed.
    # Try using network info instead. 
    if network_fields is not None:
        operator = lookup_operator_from_codes(network_fields, mobile_codes)
        if operator is not None:
            return operator
        
        # Otherwise, try the name string instead.
        operator = lookup_operator_from_field(network_fields, 'operator')
        if operator is not None:
            return operator
    
    # Lookup failed - no operator information in payload.
    return None


# Format operator name. 
# Only record counts for recognized operators.
# Pass recognized_list as a set. 
def get_operator(icc_fields, network_fields, recognized_list, mobile_codes):
    # Look up operator name either using mobile codes 
    # or from name listed in the data.
    operator = lookup_operator(icc_fields, network_fields, mobile_codes)
    if operator is None or len(operator) == 0:
        return 'Unknown'
        
    # Make formatting consistent to avoid duplication.
    # Apply replacement regexes.
    for s in subs['operator']:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit loop. 
        formatted, n = s['regex'].subn(s['repl'], operator, count = 1)
        if n > 0:
            operator = formatted
            break
    
    # Don't keep name if not in recognized list. 
    if operator not in recognized_list: 
        return 'Other'
    
    return operator


# Additional formatting to cover special cases. 
# Replacement rules draw on combination of sanitized values 
# and other raw payload values. 
def format_values(clean_values, payload):
    # Discard v1.5.
    if clean_values['os'].startswith('1.5'):
        raise ValueError('Ignoring OS version 1.5')
        
    # Tarako/India.
    # OS should either be standard or else one of the Tarako strings.
    if clean_values['os'].lower().startswith(('ind_', 'intex_')):
        # If the Tarako devices are from India, record. 
        # if clean_values['country'] == 'India':
        clean_values['os'] = '1.3T (Tarako)'
        # else: 
        # Discard.
            # raise ValueError('Ignoring non-India Tarako')
    
    return clean_values






