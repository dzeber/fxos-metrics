"""
This module contains the regexes and rules used in validity checking and
reformatting of FxOS payload field values. 

This includes regexes for converting to standardized device and operator names
from the various versions that appear across the raw dataset. This ensures that
payloads get assigned to the same segments, eg. based on device or operator, 
even if the strings used in the payloads have small differences. The standard 
names are intended to be relatively concise for use in the dashboards.

There are also formatting utilities for standardizing and summarizing the
OS version numbers. In particular, Tarako (1.3T) builds do not have a standard
version identifier, and must be identified on a case-by-case basis. For these,
the OS version is changed to '1.3T'.

The function general_formatting() is intended for miscellaneous reformatting
not covered by the other regexes (ie. correcting a field misspecified by a 
partner) for specific cases.
"""

import re
from datetime import date, timedelta


def add_suffix(name, suffix):
    """Add suffix to name separated by a space, if suffix is non-empty."""
    if len(suffix) > 0:
        return name + ' ' + suffix
    return name


#==============================================================

# Formatting functions
# --------------------


def format_tarako(datum):    
    """Special formatting for Tarako devices. 
    
    Set the OS version number to '1.3T' for devices identified as Tarako 
    based on the device name.
    """
    if ('product_model' in datum and datum['product_model'].startswith(
            ('Intex', 'Spice', 'Ace', 'Zen'))):
        datum['os'] = '1.3T'
    return datum


def general_formatting(datum):
    """General formatting based on entire record.
    
    This is intended for special cases not covered by the other regexes,
    in particular for rules depeding on multiple field values.
    """
    # OS should be 1.4 for GoFox devices. 
    if 'product_model' in datum and datum['product_model'].startswith('GoFox'):
        datum['os'] = '1.4'
    return datum


#==============================================================

# Regexes
# -------


# Form of valid OS string. 
# 1.3, 1.3T, 1.4, 2.x or 3.x.
valid_os = re.compile('^(1\.[34]|2\.[0-9]|3\.[0-9])(T|\s\(pre-release\))?$')

# Standard channels to search for in channel string.
standard_channels = re.compile('release|beta|aurora|nightly|default')

# Strip country identifier from locale code. 
locale_base_code = { 
    'regex': re.compile('-.+$'),
    'repl': ''
}


# Regexes for substitutions.
os_subs = [
    {
        'regex': re.compile('[.\-]prerelease$', re.I),
        'repl': ' (pre-release)'
    },{
        'regex': re.compile(
            '^(?P<num>[1-9]\.[0-9](\.[1-9]){0,2})(\.0){0,2}', re.I),
        'repl': '\g<num>'
    },{
        # For now, Tarako label is based on mapping from partner/device.
        'regex': re.compile('^(ind|intex)_.+$', re.I),
        'repl': '1.3T'
    }
]    

device_subs = [
    {
        # One Touch Fire. 
        'regex': re.compile(
            '^.*one\s*touch.*fire\s*(?P<suffix>[ce]?)(?:\s+\S*)?$', re.I),
        'repl': lambda match: add_suffix('One Touch Fire', 
            match.group('suffix').upper())
    },{
       # Open 2/C.
        'regex': re.compile(
            '^.*open\s*(?P<suffix>[2c])(?:\\s+\\S*)?$', re.I),
        'repl': lambda match: 'ZTE Open ' + match.group('suffix').upper()
    },{
        # Open.
        'regex': re.compile('^.*open\s*$', re.I),
        'repl': 'ZTE Open'
    },{
        # Flame.
        'regex': re.compile('^.*flame.*$', re.I),
        'repl': 'Flame'
    },{ 
        # Geeksphone.
        'regex': re.compile('^.*(keon|peak|revolution).*$', re.I),
        # 'repl': lambda match: 'Geeksphone ' + match.group(1).capitalize()
        'repl': 'Geeksphone'
    },{
        # Emulators/dev devices
        'regex': re.compile('^.*(android|aosp).*$', re.I),
        'repl': 'Emulator/Android'
    },{
        # Tarako - Cloud FX.
        'regex': re.compile('^.*clou.?d\\s*fx.*$', re.I),
        'repl': 'Intex Cloud FX'
    },{
        # Tarako - Spice.
        'regex': re.compile('^.*spice(\\s*|_)mi-?fx(?P<ver>[12]).*$', re.I),
        'repl': lambda match: 'Spice MIFX' + match.group('ver')
        # 'repl': 'Spice MIFX1'
    },{
        # Tarako - Cherry Ace.
        'regex': re.compile('^ace\\s*f100.*$', re.I),
        'repl': 'Ace F100'
    },{
        # Fire C device in Peru
        'regex': re.compile('^4019a$', re.I),
        'repl': 'One Touch Fire C'
    },{
        # Zen U105.
        'regex': re.compile('^.*u105.*$', re.I),
        'repl': 'Zen U105'
    },{
        # Fx0.
        'regex': re.compile('^lgl25.*$', re.I),
        'repl': 'Fx0'
    },{
        # Pixi 3.
        'regex': re.compile('^.*pixi\\s*3(\\s+\\(?|\\()3\\.5\\)?.*$', re.I),
        'repl': 'Pixi 3 (3.5)'
    },{
        # Orange Klif.
        'regex': re.compile('^.*klif.*$', re.I),
        'repl': 'Orange Klif'
    },{
        # Panasonic TV.
        'regex': re.compile('^ptv-.*$', re.I),
        'repl': 'Panasonic TV'
    },{
        # Sony Xperia Z3C.
        'regex': re.compile('^.*xperia\s*z3\s*c(ompact)?(\W+.*)?$', re.I),
        'repl': 'Xperia Z3C'
    }
    # ,{
        # # GoFox.
        # 'regex': re.compile('^.*gofox.*$', re.I),
        # 'repl': 'GoFox F15'
    # }
]

operator_subs = [
    # Filtering based on prefixes.
    {
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
        'regex': re.compile('^AKTel.*$', re.I),
        'repl': 'Robi'
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
        'regex': re.compile('^Banglalink.*$', re.I),
        'repl': 'Banglalink'
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
        'repl': 'Claro'
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
        'regex': re.compile('^Grameen.*$', re.I),
        'repl': 'Grameenphone'
    },{
        'regex': re.compile('^GP$', re.I),
        'repl': 'Grameenphone'
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
        'regex': re.compile('^Liaoning.*$', re.I),
        'repl': 'China Mobile'
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
        'regex': re.compile('^Robi.*$', re.I),
        'repl': 'Robi'
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
        'regex': re.compile('^TATA Teleservices.*$', re.I),
        'repl': 'Docomo'
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
        'regex': re.compile('^Videocon.*$', re.I),
        'repl': 'Videocon'
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
    },
    # More general pattern matching (eg. spelling differences).
    {
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
        'regex': re.compile('^gramee?n(phone)?$', re.I),
        'repl': 'Grameenphone'
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
    }
]

