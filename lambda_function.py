from botocore.vendored import requests
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.sql import func
import os
db = os.environ['db']


def lambda_handler(event, context):
    engine = create_engine(db)
    meta = MetaData(engine, reflect=True)
    table = meta.tables['user']
    def get_guildname(id):
        # Return Just Player Information
        r = requests.get('https://gameinfo.albiononline.com/api/gameinfo/guilds/'+id)
        return r.json()
    def get_playerguild(name):
        #Returns Guild and Alliance info. This also checks for empty users as there is a bug that someusers are listed multiple times but these 'Clones' only have 0 Fame
        rawusers = requests.get('https://gameinfo.albiononline.com/api/gameinfo/search?q='+name).json()
        guild=''
        alliance=''
        guildname=''
        lastkillfame=0
        for albionplayer in rawusers["players"]:
            if (albionplayer['Name'].lower() == name.lower()) and (albionplayer['KillFame'] >= lastkillfame):
                lastkillfame=albionplayer['KillFame']
                guild = albionplayer['GuildId']
                guildname=albionplayer['GuildName']
                if albionplayer['GuildId'] != "":
                    try:
                        alliance =get_guildname(albionplayer['GuildId'])['AllianceId']
                    except:
                        alliance=albionplayer['AllianceId']
        return {"guildid":guild, "guildname":guildname, "alliance":alliance}
    def update_player(id,playerdata):
        conn = engine.connect()
        ins = table.update().where(table.c.id==id).values(
            guildid=playerdata['guildid'], \
            guildname=playerdata['guildname'], \
            allianceid=playerdata['alliance'])
        conn.execute(ins)
        print('Updated '+ str(id))
        conn.close()
        return
    def get_waitingforverify():
        conn = engine.connect()
        rawplayers = conn.execute(select([table]).where(table.c.verified == False)).fetchall()
        conn.close()
        return rawplayers

    players=get_waitingforverify()
    print (players)
    for player in players:
        playerdata=get_playerguild(player['albionname'])
        update_player(player['id'],playerdata)

    engine.dispose()
