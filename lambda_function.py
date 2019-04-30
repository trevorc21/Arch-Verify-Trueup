from botocore.vendored import requests
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.sql import func
import os
db = os.environ['db']
access_token = os.environ['access_token']

def lambda_handler(event, context):
    headers = {"Authorization": "Bot " + access_token}
    engine = create_engine(db)
    meta = MetaData(engine, reflect=True)
    table = meta.tables['user']

    def post_accept(id):
        requests.put("https://discordapp.com/api/guilds/200746010102726657/members/"+str(id)+"/roles/530223486694719558",data=None, headers=headers)
        requests.put("https://discordapp.com/api/guilds/200746010102726657/members/"+str(id)+"/roles/"+guildrole(playerdata['guildid']),data=None, headers=headers)
        guildtag=getguildtag(playerdata['guildid'])
        requests.patch("https://discordapp.com/api/guilds/200746010102726657/members/"+str(id),data=None, json={'nick':'['+guildtag+'] '+player['albionname']}, headers=headers)
        return
    def get_guildname(id):
        r = requests.get('https://gameinfo.albiononline.com/api/gameinfo/guilds/'+id)
        return r.json()


    def createguildtag(guildname):
        try:
            return guildname[:4].upper()
        except:
            return 'NONE'


    def getguildtag(guildid):
        conn = engine.connect()
        result = conn.execute("Select tag from guilds where guildid = '"+guildid+"'").fetchone()
        conn.close()
        if result:
            return result[0]

        return 'NONE'


    def guildrole(guildid):
        conn = engine.connect()
        result = conn.execute("Select discordid from guilds where guildid = '"+guildid+"'").fetchone()
 
        if result:
            conn.close()
            return result[0]
        else:
            if not guildid:
                return '530223486694719558'
            guildinfo =get_guildname(guildid)

            post_data = {"name": guildinfo['Name'], "mentionable" : True}
            headers = {"Authorization": "Bot " + access_token}
            response=requests.post("https://discordapp.com/api/guilds/200746010102726657/roles",data=None,json=post_data, headers=headers).json()
            engine.execute("INSERT INTO `guilds` (guildid, discordid,guildname,tag) VALUES ('{0}','{1}','{2}','{3}' )".format(guildid,response['id'],guildinfo['Name'],createguildtag(guildinfo['Name'])))
            conn.close()
            return response['id']


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
        if playerdata['alliance']=='hRqowi9bTw6o44R0bsmIUw':
            try:
                post_accept(id)
                ins = table.update().where(table.c.id==id).values(
                    verified=True)
                conn.execute(ins)
            except:
                print("Failed")
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
