import argparse
import re
from collections import defaultdict

import json
import time
import sys

from datetime import datetime

last_reqnumber_per_worker = defaultdict(int)
last_reqnumber_per_core = defaultdict(int)

def human_size(n):
    # G
    if n >= (1024*1024*1024):
        return "%.1fG" % (n/(1024*1024*1024))
    # M
    if n >= (1024*1024):
        return "%.1fM" % (n/(1024*1024))
    # K
    if n >= 1024:
        return "%.1fK" % (n/1024)
    return "%d" % n

def reqcount(item):
    return item['requests']


def calc_percent(tot, req):
    if tot == 0:
        return 0.0
    return (100 * float(req)) / float(tot)

def merge_worker_with_cores(workers, rps_per_worker, cores, rps_per_core):
    workers_by_id = dict([(w['id'], w) for w in workers])
    new_workers = []
    for wid, w_cores in cores.items():
        for core in w_cores:
            cid = core['id']
            data = dict(workers_by_id.get(wid))
            data.update(core)
            if data['status'] == 'busy' and not core['in_request']:
                data['status'] = '-'
            new_wid = "{0}:{1}".format(wid, cid)
            data['id'] = new_wid
            rps_per_worker[new_wid] = rps_per_core[wid, cid]
            new_workers.append(data)
    workers[:] = new_workers

"""
def stdout(msg, screen=None, row=0, col=0, attr=None):
  if screen != None:
    screen.addstr(row, col, msg, attr)
  else:
    print(msg)
"""

def decode(dd, current_tot_time, last_tot_time, stdout, host=None, async_mode=2):
  uversion = ''

  if 'version' in dd:
      uversion = '-' + dd['version']

  if 'listen_queue' not in dd:
      dd['listen_queue'] = 0

  cwd = ""
  if 'cwd' in dd:
      cwd = "- cwd: %s" % dd['cwd']

  uid = ""
  if 'uid' in dd:
      uid = "- uid: %d" % dd['uid']

  gid = ""
  if 'gid' in dd:
      gid = "- gid: %d" % dd['gid']

  masterpid = ""
  if 'pid' in dd:
      masterpid = "- masterpid: %d" % dd['pid']

  stdout("node: %s %s %s %s %s" % (host, cwd, uid, gid, masterpid), 1, 0)

  if 'vassals' in dd:
      stdout("uwsgi%s - %s - emperor: %s - tyrant: %d" % (uversion, time.ctime(), dd['emperor'], dd['emperor_tyrant']),  0, 0)
      if dd['vassals']:
          vassal_spaces = max([len(v['id']) for v in dd['vassals']])
          stdout(" VASSAL%s\tPID\t" % (' ' * (vassal_spaces-6)), 2, 0, curses.A_REVERSE if "curses" in sys.modules else None)
          pos = 3
          for vassal in dd['vassals']:
              stdout(" %s\t%d" % (vassal['id'].ljust(vassal_spaces), vassal['pid']), pos, 0)
              pos += 1

  elif 'workers' in dd:
      tot = sum([worker['requests'] for worker in dd['workers']])

      rps_per_worker = {}
      rps_per_core = {}
      cores = defaultdict(list)
      dt = current_tot_time - last_tot_time
      total_rps = 0
      for worker in dd['workers']:
          wid = worker['id']
          curr_reqnumber = worker['requests']
          last_reqnumber = last_reqnumber_per_worker[wid]

          rps_per_worker[wid] = (curr_reqnumber - last_reqnumber) / dt
          last_reqnumber_per_worker[wid] = curr_reqnumber
          
          if not async_mode:
              continue
          for core in worker.get('cores', []):
              if not core['requests']:
                  # ignore unused cores
                  continue
              wcid = (wid, core['id'])
              curr_reqnumber = core['requests']
              last_reqnumber = last_reqnumber_per_core[wcid]
              rps_per_core[wcid] = (curr_reqnumber - last_reqnumber) / dt
              last_reqnumber_per_core[wcid] = curr_reqnumber
              cores[wid].append(core)
          cores[wid].sort(key=reqcount)

      last_tot_time = time.time()

      if async_mode == 1:
          merge_worker_with_cores(dd['workers'], rps_per_worker,
                                  cores, rps_per_core)

      tx = human_size(sum([worker['tx'] for worker in dd['workers']]))

      stdout("uwsgi%s - %s - req: %d - RPS: %d - lq: %d - tx: %s" % (uversion, time.ctime(), tot, int(round(total_rps)), dd['listen_queue'], tx), 0, 0)
      stdout(" WID\t%\tPID\tREQ\tRPS\tEXC\tSIG\tSTATUS\tAVG\tRSS\tVSZ\tTX\tReSpwn\tHC\tRunT\tLastSpwn", 2, 0, curses.A_REVERSE if "curses" in sys.modules else None)

      pos = 3

      dd['workers'].sort(key=reqcount, reverse=True)
      for worker in dd['workers']:
          sigs = 0
          wtx = human_size(worker['tx'])
          wlastspawn = "--:--:--"

          wrunt = worker['running_time']/1000
          if wrunt > 9999999:
              wrunt = "%sm" % str(wrunt / (1000*60))
          else:
              wrunt = str(wrunt)

          if worker['last_spawn']:
              wlastspawn = time.strftime("%H:%M:%S", time.localtime(worker['last_spawn']))

          color = None
          if "curses" in sys.modules:
            color = curses.color_pair(0)
            if 'signals' in worker:
                sigs = worker['signals']
            if worker['status'] == 'busy':
                color = curses.color_pair(1)
            if worker['status'] == 'cheap':
                color = curses.color_pair(2)
            if worker['status'].startswith('sig'):
                color = curses.color_pair(4)
            if worker['status'] == 'pause':
                color = curses.color_pair(5)

          wid = worker['id']

          rps = int(round(rps_per_worker[wid]))

          try:
            stdout(" %s\t%.1f\t%d\t%d\t%d\t%d\t%d\t%s\t%dms\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
                    wid, calc_percent(tot, worker['requests']),  worker['pid'], worker['requests'], rps, worker['exceptions'], sigs, worker['status'],
                    worker['avg_rt']/1000, human_size(worker['rss']), human_size(worker['vsz']),
                    wtx, worker['respawn_count'], worker['harakiri_count'], wrunt, wlastspawn
                ), pos, 0, color)
          except Exception as e:
              pass
          pos += 1
          if async_mode != 2:
              continue
          for core in cores[wid]:
              if "curses" in sys.modules:
                color = curses.color_pair(0)
              if core['in_request']:
                  status = 'busy'

                  if "curses" in sys.modules:
                    color = curses.color_pair(1)
              else:
                  status = 'idle'

              cid = core['id']
              rps = int(round(rps_per_core[wid, cid]))
              try:
                  stdout("  :%s\t%.1f\t-\t%d\t%d\t-\t-\t%s\t-\t-\t-\t-\t-" % (
                        cid, calc_percent(tot, core['requests']),  core['requests'], rps, status,
                    ), pos, 0, color)
              except:
                  pass
              pos += 1

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('logfile', type=str, help='Original Log file')

  args = parser.parse_args()
  logfile = args.logfile

  #screen = curses.initscr()
  #curses.noecho()
  #curses.start_color()
  #curses.use_default_colors()

  with open(logfile, "r") as fd:
    data = fd.read()

  matches = re.findall(r'Script started on (\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2})\+\d{2}:\d{2}.+([\s\S]*?)Script done on (\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2})\+\d{2}:\d{2}', str(data))
  
  
  last_tot_time = -1
  stdout = lambda msg, *args: print(msg)
  
  for match in matches:
    try:
      print(match[0])
      date = datetime.strptime(match[0], "%Y-%m-%d %H:%M:%S")
      base_sec = (date - datetime(1970,1,1)).total_seconds()

      if last_tot_time < 0:
        last_tot_time = base_sec
        continue

      log = json.loads(match[1])
      decode(log, base_sec, last_tot_time, stdout)
      last_tot_time = base_sec
    except json.decoder.JSONDecodeError:
      print(f"skip data:{match[1]}")
  





