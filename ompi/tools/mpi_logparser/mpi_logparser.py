#!/usr/bin/env python
import sys
import os
import re
import subprocess
import threading
import time

# log level
LOGGER_BASE = 0
LOGGER_ERR = 1
LOGGER_DEBUG = 2

# log type
LOG_TYPE_UD_TMOUT = 1 << 1
LOG_TYPE_RC_TMOUT = 1 << 2
LOG_TYPE_UCP_RECV_TMOUT = 1 << 3
LOG_TYPE_UCP_SEND_TMOUT = 1 << 4
LOG_TYPE_PEER_NAME = 1 << 5

# minimum upper length of the output list.
LIST_OUTPUT_UPPER_MIN = 5

# maximum upper length of the output list.
LIST_OUTPUT_UPPER_MAX = 500000

# fixed redirect output file
OUTPUT_FILE_NAME = 'output.log'

# cmd line return code
CMD_OK = 0
CMD_TIMEOUT = -1

#
# Python versions vary according to OS.
# We must consider python compatibility.
#


class PyVersion:
    def __init__(self):
        self.major = sys.version_info.major
        self.minor = sys.version_info.minor
        self.micro = sys.version_info.micro

    def __str__(self):
        return "current python version: {}.{}.{}".format(self.major, self.minor, self.micro)

    def major(self):
        return self.major


#
# CMD arg parser, consider python compatibility.
# Here, we support to parse file or directory, use:
#   mpi_logparser [options]
#       -f FILE, --file FILE            Independent MPI log file
#       -d DIR, --dir DIR               MPI log directory, formed by MPI --output-filename
#       -o OUTPUT, --output OUTPUT      Log analyzer redirection directory
#       -u UPPER, --upper UPPER         List output upper limit, default value 5
#       -l LEVEL, --log_level LEVEL     Log level, 1: err, 2: debug, default value 1
#       -s, --skip_gid_parsing          Skip gid parsing, because gid parsing increases analysis time.
#


class ArgParser:
    def __init__(self):
        try:
            import argparse
        except ImportError:
            import optparse as argparse
        if hasattr(argparse, 'ArgumentParser'):
            self.parser = argparse.ArgumentParser(prog='mpi_logparser',
                                                  usage='%(prog)s [options]',
                                                  description='MPI log parser tools.\n' \
                                                              'Currently, only timeout logs can be parsed.',
                                                  epilog='Either file or dir must be specified.\n' \
                                                         'If both are specified, the file will take effect.')
            self.parser.add_argument('-f', '--file', type=str, dest='file',
                                     help='Independent MPI log file')
            self.parser.add_argument('-d', '--dir', type=str, dest='dir',
                                     help='MPI log directory, formed by MPI --output-filename args')
            self.parser.add_argument('-o', '--output', type=str, dest='output',
                                     help='Log analyzer redirection directory')
            self.parser.add_argument('-u', '--upper', type=int, dest='upper', default=LIST_OUTPUT_UPPER_MIN,
                                     help='List output upper limit, value range [{0}, {1}], default value {0}'.format(
                                     LIST_OUTPUT_UPPER_MIN, LIST_OUTPUT_UPPER_MAX))
            self.parser.add_argument('-l', '--log_level', type=int, dest='level', default=1,
                                     help='Log level, 1: err, 2: debug, default value 1')
            self.parser.add_argument('-s', '--skip_gid_parsing', action='store_true',
                                     help='Skip gid parsing, because gid parsing increases analysis time.')
            self.args = self.parser.parse_args()
        else:
            self.parser = argparse.OptionParser(prog='mpi_logparser',
                                                usage='%(prog)s [options]',
                                                description='MPI log parser tools.\n' \
                                                            'Currently, only timeout logs can be parsed.',
                                                epilog='Either file or dir must be specified.\n' \
                                                         'If both are specified, the file will take effect.')
            self.parser.add_option('-f', '--file', type="string", dest='file',
                                   help='Independent MPI log file')
            self.parser.add_option('-d', '--dir', type="string", dest='dir',
                                   help='MPI log directory, formed by MPI --output-filename args')
            self.parser.add_option('-o', '--output', type="string", dest='output',
                                   help='Log analyzer redirection directory')
            self.parser.add_option('-u', '--upper', type=int, dest='upper', default=LIST_OUTPUT_UPPER_MIN,
                                   help='List output upper limit, value range [{0}, {1}], default value {0}'.format(
                                   LIST_OUTPUT_UPPER_MIN, LIST_OUTPUT_UPPER_MAX))
            self.parser.add_option('-l', '--log_level', type="int", dest='level', default=1,
                                   help='Log level, 1: err, 2: debug, default value 1')
            self.parser.add_option('-s', '--skip_gid_parsing', action='store_true', dest="skip_gid_parsing",
                                   default=False, help='Skip gid parsing')
            self.args, _ = self.parser.parse_args()

    def get_args(self):
        return self.args

    def get_log_level(self):
        # Round down args.level
        # If args.level exceeds the range, set it to the default value.
        self.args.level = int(self.args.level)
        if self.args.level > 2 or self.args.level < 1:
            self.args.level = 1
        if self.args.level == 1:
            return LOGGER_ERR
        return LOGGER_DEBUG

    def dump_help(self):
        self.parser.print_help()

#
# Simple encapsulation of linux command Line commands
#


class CmdRunner:
    @staticmethod
    def run(cmd, timeout=1, **kwargs):
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, shell=True, **kwargs)
        if PyVersion().major > 2:
            try:
                stdout, stderr = proc.communicate(timeout=timeout)
                return 0, stdout, stderr
            except subprocess.TimeoutExpired:
                proc.terminate()
                return -1, "", ""
        else:
            def kill_process():
                try:
                    proc.terminate()
                except Exception:
                    # resources have been released, skip
                    pass
            timer = threading.Timer(timeout, kill_process)
            timer.start()
            try:
                stdout, stderr = proc.communicate()
                return 0, stdout, stderr
            finally:
                timer.cancel()


#
# Log wrapper
#


class Logger:
    def __init__(self, level=LOGGER_ERR, f=None):
        self.level = level
        if f is None:
            self.f = sys.stdout
        else:
            self.f = f

    def log(self, level, format_str):
        if level <= self.level:
            self.f.write(format_str)

    def log_base(self, format_str):
        self.log(LOGGER_BASE, "{}\n".format(format_str))

    def log_err(self, format_str):
        self.log(LOGGER_ERR, "[LOG_PARSER][ERR]{}\n".format(format_str))
    
    def log_debug(self, format_str):
        self.log(LOGGER_DEBUG, "[LOG_PARSER][DEBUG]{}\n".format(format_str))


#
# file parser
#


class FileParser:
    def __init__(self, logger, f, analyser):
        self.logger = logger
        self.f = f
        self.analyser = analyser

    def parse(self):
        try:
            fh = open(self.f, 'r')
        except Exception as e:
            self.logger.log_err("cannot open log file({}):{}".format(self.f, str(e)))
            return 0

        for line in fh:
            self.analyser.analyse(line)
        fh.close()
        return 1


#
# directory parser
# only support the files name 'stdout' or 'stderr', generated by mpirun args '--output-filename <dir>'
#


class DirParser:
    def __init__(self, logger, d, analyser):
        self.logger = logger
        self.d = d
        self.analyser = analyser
        self.file_matches = ['stdout', 'stderr']

    def parse(self):
        for root, dirs, files in os.walk(self.d):
            for f in files:
                if f not in self.file_matches:
                    continue
                file_path = os.path.join(root, f)
                try:
                    fh = open(file_path, 'r')
                except Exception as e:
                    self.logger.log_err("cannot open log file({}):{}".format(file_path, str(e)))
                    continue
                for line in fh:
                    self.analyser.analyse(line)
                fh.close()
        return 1


#
# a simple diGraph interface, only provides in_degree and out_degree
#


class DiGraph:
    def __init__(self):
        # to save p2p relation, value is set for deduplicating
        self.graph = {}
        # to save every point's in_degree
        self._in_degree = {}

    # add an edge to graph
    def add_edge(self, src, dst):
        if dst not in self.graph:
            self.graph[dst] = set()
        if src not in self.graph:
            self.graph[src] = set()
        self.graph[src].add(dst)

        if src not in self._in_degree:
            self._in_degree[src] = 0
        if dst not in self._in_degree:
            self._in_degree[dst] = 0
        self._in_degree[dst] += 1

    def in_degree(self, n):
        return self._in_degree[n]

    def out_degree(self, n):
        return len(self.graph[n])

    def out_nodes(self, n):
        return self.graph[n]

    def nodes(self):
        return self.graph.keys()

#
# record rank info
#


class RankInfo:
    def __init__(self, rank, hostname):
        self.rank = rank
        self.hostname = hostname

    def __str__(self):
        return "rank{}({})".format(self.rank, self.hostname)
    
    def dump(self):
        return "rank{}({})".format(self.rank, self.hostname)

#
# record dev pair info
#


class DevPairInfo:
    def __init__(self, vpid):
        self.vpid = vpid
        self.local_hostname = ""
        self.remote_hostname = ""
        self.local_dev_name = ""
        # remote_dev_gid maybe store gid for Ethernet, or maybe store lid for InfiniBand
        self.remote_dev_gid = ""

    def set_hostname(self, local_hostname, remote_hostname):
        self.local_hostname = local_hostname
        self.remote_hostname = remote_hostname

    def set_devinfo(self, local_dev_name, remote_dev_gid):
        self.local_dev_name = local_dev_name
        # lid's type is uint16_t, max is 65535
        if len(remote_dev_gid) > 6:
            self.remote_dev_gid = remote_dev_gid[4:6] + ':' + remote_dev_gid[2:4] + remote_dev_gid[:2]
        else:
            self.remote_dev_gid = remote_dev_gid

    def get_remote_hostname(self):
        return self.remote_hostname

    def get_local_dev_name(self):
        return self.local_dev_name

    def get_remote_dev_gid(self):
        return self.remote_dev_gid

#
# record host fault ref info
#


class HostInfo:
    def __init__(self, hostname):
        self.hostname = hostname
        self.refs = 0
        self.dev_refs = {}
        self.gid_refs = {}

    def add_refs(self):
        self.refs += 1

    def add_dev(self, dev, is_gid):
        # gid format: 'xx:xxxx'
        # lid format: 'xx'
        if is_gid:
            if dev not in self.gid_refs:
                self.gid_refs[dev] = 0
            self.gid_refs[dev] += 1
            return
        if dev not in self.dev_refs:
            self.dev_refs[dev] = 0
        self.dev_refs[dev] += 1

    def get_refs(self):
        return self.refs

    def get_devs(self):
        return self.dev_refs

    def get_dev_ref(self, dev):
        return self.dev_refs[dev]

    def get_hostname(self):
        return self.hostname

    def hostname_is_valid(self):
        hostname = self.hostname
        if not hostname or len(hostname) > 255:
            return False

        if hostname.endswith('.'):
            hostname = hostname[:-1]

        labels = hostname.split('.')
        label_pattern = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$', re.IGNORECASE)

        for label in labels:
            if not label_pattern.match(label):
                return False
            if label.startswith('-') or label.endswith('-'):
                return False

        return True

    # parse dev gid to dev name, need ssh pwd-free
    def parse_dev_gid(self, skip_gid_parsing):
        for dev_gid in self.gid_refs:
            ret = 0
            out = ""
            if not self.hostname_is_valid():
                skip_gid_parsing = 1
            if not skip_gid_parsing:
                ret, out, _ = CmdRunner.run("ssh -o PasswordAuthentication=no -o UserKnownHostsFile=/dev/null " \
                                            "-o StrictHostKeyChecking=no {} \"ibv_devinfo -v " \
                                            "| grep -E 'hca_id|GID|port_lid'\"".format(self.hostname), timeout=1)
            check = 0
            if not ret and out:
                dev_infos = []
                try:
                    dev_infos = out.split('\n')
                except TypeError:
                    dev_infos = out.decode().split('\n')
                for line in dev_infos:
                    result = re.match(r"^hca_id:[\s\t]+([^\s]+)\r?\n?$", line, 0)
                    # parse dev name
                    if result:
                        ib_dev_name = result.group(1)
                        continue
                    # parse lid
                    result = re.match(r"^[\s\t]*port_lid:[\s\t]+([\d]+)\r?\n?$", line, 0)
                    if result:
                        if dev_gid == result.group(1):
                            if ib_dev_name not in self.dev_refs:
                                self.dev_refs[ib_dev_name] = 0
                            self.dev_refs[ib_dev_name] += self.gid_refs[dev_gid]
                            check = 1
                            break
                    # parse gid
                    elif dev_gid in line:
                        if ib_dev_name not in self.dev_refs:
                            self.dev_refs[ib_dev_name] = 0
                        self.dev_refs[ib_dev_name] += self.gid_refs[dev_gid]
                        check = 1
                        break
            if check == 0:
                if dev_gid not in self.dev_refs:
                    self.dev_refs[dev_gid] = 0
                self.dev_refs[dev_gid] += self.gid_refs[dev_gid]

#
# data analyser
#


class LogAnalyser:
    def __init__(self, logger, list_output_upper_limit=LIST_OUTPUT_UPPER_MIN, skip_gid_parsing=0):
        self.logger = logger
        # ucp recv tmout
        # we use comm domain for key, every comm domain has own map
        self.digraphs = {}      # record p2p direction
        self.node_maps = {}     # record rank -> hostname
        # uct tmout
        self.proc_tmo_infos = {}     # record ud/rc hostname -> {vpid : DevPairInfo}
        # we need to filter some of the logs we don't need.
        self.log_whitelist = [
            {
                # (local_host, vpid, local_dev_name, remote_dev_gid)
                'format': re.compile(r'^\[([^:]+):(\d+):\d+:\d+\][\s\t]+ud_ep\.c.*Fatal: UD endpoint' \
                                     r'.*unhandled timeout error with local dev name:([^\s]+) ' \
                                     r'remote dev gid.* interface id:([0-9a-f]+)\]\r?\n?$'),
                'type': LOG_TYPE_UD_TMOUT
            },
            {
                # (local_host, vpid, local_dev_name, remote_dev_gid)
                'format': re.compile(r'^\[([^:]+):(\d+):\d+:\d+\][\s\t]+ud_ep\.c.*Fatal: UD endpoint' \
                                     r'.*unhandled timeout error with local dev name:([^\s]+) ' \
                                     r'remote dev lid:\[([0-9a-f]+)\]\r?\n?$'),
                'type': LOG_TYPE_UD_TMOUT
            },
            {
                # (local_host, vpid, local_dev_name, remote_dev_gid)
                'format': re.compile(r'^\[([^:]+):(\d+):\d+:\d+\][\s\t]+RC unhandled timeout error.* ' \
                                     r'dev name:([^\s]+).*interface id:([0-9a-f]+)\]\r?\n?$'),
                'type': LOG_TYPE_RC_TMOUT
            },
            {
                # (local_host, vpid, local_dev_name, remote_dev_gid)
                'format': re.compile(r'^\[([^:]+):(\d+):\d+:\d+\][\s\t]+RC unhandled timeout error.* ' \
                                     r'dev name:([^\s]+) remote dev lid:\[([0-9a-f]+)\]\r?\n?$'),
                'type': LOG_TYPE_RC_TMOUT
            },
            {
                # (local_host, vpid, remote_host)
                'format': re.compile(r'^\[[\d\.]+\] \[([^:]+):(\d+):\d+\][\s\t]+ucp_worker\.c.*' \
                                     r'UCT send timeout peer_hostname: ([^\s]+)\r?\n?$'),
                'type': LOG_TYPE_PEER_NAME
            },
            {
                # (local_host, tag, local_rank, remote_rank, remote_host)
                'format': re.compile(r'^\[([^:]+):\d+\].*UCP recv request timeout! request tag ([0-9A-Fx]+) ' \
                                     r'local proc: (\d+) peer proc: (\d+) peer hostname: ([^\s]+)\r?\n?$'),
                'type': LOG_TYPE_UCP_RECV_TMOUT
            }
        ]
        self.log_type = 0
        if list_output_upper_limit > LIST_OUTPUT_UPPER_MIN:
            self.list_output_upper_limit = list_output_upper_limit
        else:
            self.list_output_upper_limit = LIST_OUTPUT_UPPER_MIN
        self.skip_gid_parsing = skip_gid_parsing

    def __analyse_ucp_recv_tmout(self, re_result):
        # parse
        local_hostname, tag_str, local_rank_str, peer_rank_str, peer_hostname = re_result.groups()
        tag = int(tag_str, 16)
        local_rank = int(local_rank_str)
        peer_rank = int(peer_rank_str)
        comm_domain = tag & 0xFFFFFF00000FFFFF
        # every comm domain has own graph
        if comm_domain not in self.digraphs:
            self.digraphs[comm_domain] = DiGraph()
        if comm_domain not in self.node_maps:
            self.node_maps[comm_domain] = {}
        # because this is recv timeout, direction of P2P is peer -> local
        self.digraphs[comm_domain].add_edge(peer_rank, local_rank)
        if local_rank not in self.node_maps[comm_domain]:
            self.node_maps[comm_domain][local_rank] = local_hostname
        if peer_rank not in self.node_maps[comm_domain]:
            self.node_maps[comm_domain][peer_rank] = peer_hostname

    def __analyse_recv_tmout(self, re_result):
        # parse
        local_hostname, vpid, local_dev_name, remote_dev_gid = re_result.groups()
        if local_hostname not in self.proc_tmo_infos:
            self.proc_tmo_infos[local_hostname] = {vpid: DevPairInfo(vpid)}
        elif vpid not in self.proc_tmo_infos[local_hostname]:
            self.proc_tmo_infos[local_hostname][vpid] = DevPairInfo(vpid)
        self.proc_tmo_infos[local_hostname][vpid].set_devinfo(local_dev_name, remote_dev_gid)

    def __analyse_peer_name(self, re_result):
        # parse
        local_hostname, vpid, peer_hostname = re_result.groups()
        if local_hostname not in self.proc_tmo_infos:
            self.proc_tmo_infos[local_hostname] = {vpid: DevPairInfo(vpid)}
        elif vpid not in self.proc_tmo_infos[local_hostname]:
            self.proc_tmo_infos[local_hostname][vpid] = DevPairInfo(vpid)
        self.proc_tmo_infos[local_hostname][vpid].set_hostname(local_hostname, peer_hostname)

    def __ucp_recv_tmout_dump(self):
        self.logger.log_base("* UCP RECV TIMEOUT ")
        self.logger.log_base("* Brief Exception Information ")
        self.logger.log_base("* The exception may originate from the following nodes: ")
        for comm_domain, digraph in self.digraphs.items():
            # debug info
            s = sorted(digraph.nodes(), key=lambda x: digraph.in_degree(x))
            self.logger.log_debug("(rank, indegree, [out_ranks]: {})".format(
                [(rank, digraph.in_degree(rank), list(digraph.out_nodes(rank))) for rank in s]))
            # For all nodes with indegree of 0, we suspect that they are faulty,
            # the corresponding endpoints connected to them also maybe faulty.
            in_degree_0_ranks = [x for x in digraph.nodes() if digraph.in_degree(x) == 0]

            if len(in_degree_0_ranks) > 0:
                self.logger.log_base("** comm domain {}".format(hex(comm_domain)))
                for send_rank in in_degree_0_ranks:
                    recv_ranks = digraph.out_nodes(send_rank)
                    if len(recv_ranks) > self.list_output_upper_limit:
                        recv_ranks = list(recv_ranks)[:self.list_output_upper_limit]
                    if len(recv_ranks) > 0:
                        for recv_rank in recv_ranks:
                            self.logger.log_base("{} -> {}".format(RankInfo(send_rank, 
                                                                            self.node_maps[
                                                                            comm_domain][
                                                                            send_rank]),
                                                                   RankInfo(recv_rank,
                                                                            self.node_maps[
                                                                            comm_domain][
                                                                            recv_rank])))
        
        self.logger.log_base("* UCP RECV TIMEOUT ")
        self.logger.log_base("* Detailed Exception Information ")
        for comm_domain, digraph in self.digraphs.items():
            self.logger.log_base("** comm domain {}".format(hex(comm_domain)))
            # debug info
            s = sorted(digraph.nodes(), key=lambda x: digraph.in_degree(x))
            self.logger.log_debug("(rank, indegree, [out_ranks]: {})".format(
                [(rank, digraph.in_degree(rank), list(digraph.out_nodes(rank))) for rank in s]))
            # For all nodes with indegree of 0, we suspect that they are faulty,
            # the corresponding endpoints connected to them also maybe faulty.
            in_degree_0_ranks = [x for x in digraph.nodes() if digraph.in_degree(x) == 0]
            if len(in_degree_0_ranks) == 1:
                send_rank = in_degree_0_ranks[0]
                # special case
                if digraph.out_degree(send_rank) == 1:
                    recv_rank = list(digraph.out_nodes(send_rank))[0]
                    self.logger.log_base("There is a high probability that " \
                                         "this problem occurs between {} -> {}, " \
                                         "please check communication path of the two ranks.".format(RankInfo(send_rank,
                                                                                                        self.node_maps[
                                                                                                        comm_domain][
                                                                                                        send_rank]),
                                                                                                    RankInfo(recv_rank,
                                                                                                        self.node_maps[
                                                                                                        comm_domain][
                                                                                                        recv_rank])))
                else:
                    recv_ranks = digraph.out_nodes(send_rank)
                    recv_hosts = set()
                    for recv_rank in recv_ranks:
                        recv_hosts.add(self.node_maps[comm_domain][recv_rank])
                    self.logger.log_base("The problem may occur on {}, " \
                                         "{} ranks need to wait for rank{}'s data," \
                                         " they are distributed on {} node(s).".format(RankInfo(send_rank,
                                                                                       self.node_maps[comm_domain][
                                                                                           send_rank]),
                                                                                       len(recv_ranks),
                                                                                       send_rank, len(recv_hosts)))
                    if len(recv_ranks) > self.list_output_upper_limit:
                        recv_ranks = list(recv_ranks)[:self.list_output_upper_limit]
                    self.logger.log_base(
                        "recv ranks include(list up to {}): [{}]".format(self.list_output_upper_limit, ",".join(
                            [RankInfo(str(n), self.node_maps[comm_domain][recv_rank]).dump() for n in recv_ranks])))
            # It's not usually the case, but it's something we need to take into account.
            elif len(in_degree_0_ranks) == 0:
                self.logger.log_base("There is no top dependency, the problem may occur at link layer, or may occur" \
                                     " when resources are waiting for each other." \
                                     " This often happends when both processes are recv data" \
                                     " from the opposite, but not finishing.")

            else:
                send_hosts = set()
                recv_hosts = set()
                for send_rank in in_degree_0_ranks:
                    send_hosts.add(self.node_maps[comm_domain][send_rank])
                    for recv_rank in digraph.out_nodes(send_rank):
                        recv_hosts.add(self.node_maps[comm_domain][recv_rank])
                extra_str = ""
                if len(send_hosts) > 1 and len(recv_hosts) > 1:
                    extra_str = " Maybe it's not just a single point of failure, " \
                                "try to reduce the scale to narrow the problem " \
                                "or consider excluding the current node below."
                self.logger.log_base("Data of {} ranks is not received, covers {} send nodes, " \
                                     "{} recv nodes.".format(len(in_degree_0_ranks), len(send_hosts),
                                                               len(recv_hosts)) + extra_str)
                send_ranks = in_degree_0_ranks
                if len(send_ranks) > self.list_output_upper_limit:
                    send_ranks = list(send_ranks)[:self.list_output_upper_limit]
                if len(send_hosts) > self.list_output_upper_limit:
                    send_hosts = list(send_hosts)[:self.list_output_upper_limit]
                if len(recv_hosts) > self.list_output_upper_limit:
                    recv_hosts = list(recv_hosts)[:self.list_output_upper_limit]
                self.logger.log_base("send ranks include(list up to {}): [{}]".format(
                    self.list_output_upper_limit, ",".join(
                    [RankInfo(str(n), self.node_maps[comm_domain][recv_rank]).dump() for n in send_ranks])))
                self.logger.log_base("send nodes include(list up to {}): [{}]".format(
                    self.list_output_upper_limit, ",".join(
                    [str(n) for n in send_hosts])))
                self.logger.log_base("recv nodes include(list up to {}): [{}]".format(
                    self.list_output_upper_limit, ",".join(
                    [str(n) for n in recv_hosts])))

            self.logger.log_base("**")
        self.logger.log_base("*")

    #
    # The output content of UD is similar to that of RC.
    #


    def __uct_tmout_dump(self, log_type):
        hostInfos = {}  # record hostname -> HostInfo
        log_loss = 0    # record whether log loss
        if log_type == LOG_TYPE_UD_TMOUT:
            self.logger.log_base("* UCT UD TIMEOUT ")
        else:
            # LOG_TYPE_RC_TMOUT
            self.logger.log_base("* UCT RC TIMEOUT ")
        # record the number of errors on each node.
        self.logger.log_base("* Brief Exception Information ")
        self.logger.log_base("**")
        dev_vis = {}
        for hostname, pairs in self.proc_tmo_infos.items():
            for _, devPairInfo in pairs.items():
                remote_hostname = devPairInfo.get_remote_hostname()
                remote_dev_gid = devPairInfo.get_remote_dev_gid()
                local_dev_gid = devPairInfo.get_local_dev_name()
                if remote_dev_gid != '' and local_dev_gid != '':
                    if (local_dev_gid, remote_dev_gid) not in dev_vis:
                        dev_vis[(local_dev_gid, remote_dev_gid)] = 1
                        self.logger.log_base("local:{}:{} -> remote:{}:{}".format(hostname,
                                                                                  local_dev_gid,
                                                                                  remote_hostname,
                                                                                  remote_dev_gid))
                if len(remote_hostname) == 0 or len(remote_dev_gid) == 0:
                    log_loss = 1
                    continue
                if hostname not in hostInfos:
                    hostInfos[hostname] = HostInfo(hostname)
                hostInfos[hostname].add_refs()
                if remote_hostname not in hostInfos:
                    hostInfos[remote_hostname] = HostInfo(remote_hostname)
                hostInfos[remote_hostname].add_refs()
                hostInfos[hostname].add_dev(devPairInfo.get_local_dev_name(), 0)
                hostInfos[remote_hostname].add_dev(remote_dev_gid, 1)
        # parse dev gid
        for _, hostInfo in hostInfos.items():
            hostInfo.parse_dev_gid(self.skip_gid_parsing)
        # output
        if len(hostInfos.keys()):
            host_sort = sorted(hostInfos.keys(), key=lambda x: hostInfos.get(x, {}).get_refs(), reverse=True)
            self.logger.log_debug("host ref sort: [{}]".format(
                                  ",".join([x + ':' + str(hostInfos.get(x, {}).get_refs()) for x in host_sort])))

            self.logger.log_base("* Detailed Exception Information ")
            self.logger.log_base("**")
            for host in host_sort:
                self.logger.log_debug("host {} dev: {}".format(host, hostInfos[host].get_devs()))
            if len(host_sort) > self.list_output_upper_limit:
                host_sort = host_sort[:self.list_output_upper_limit]
            self.logger.log_base("Timeout involves {} node(s)(list up to {}): [{}]".format(
                                 len(hostInfos.keys()), self.list_output_upper_limit, ",".join(
                                 [x + '(' + str(hostInfos.get(x, {}).get_refs()) + ')' for x in host_sort])))
            for host in host_sort:
                dev_sorted = sorted(hostInfos[host].get_devs().keys(), key=lambda x: hostInfos[host].get_dev_ref(x),
                                    reverse=True)
                n = len(dev_sorted)
                if n > self.list_output_upper_limit:
                    dev_sorted = dev_sorted[:self.list_output_upper_limit]
                self.logger.log_base("node {} involves about {} dev(s)(list up to {}): [{}]".format(host, n,
                                     self.list_output_upper_limit,
                                     ",".join([x + '(' + str(
                                     hostInfos[host].get_dev_ref(x)) + ')' for x in dev_sorted])))
            self.logger.log_base("**")
            self.logger.log_base("This shows the part of the nodes where the timeout log is located, " \
                                 "the number of errors is in descending order: ")
            self.logger.log_base("\tIf the number of errors on the head node is much greater than " \
                                 "that on the subsequent nodes, there is a high probability that the node is faulty. " \
                                 "In this case, you can locate the fault on the device of the node.")
            if log_type == LOG_TYPE_UD_TMOUT:
                self.logger.log_base("\tIf the number of node errors is evenly distributed, "\
                                    "link layer may be slow. In this case, "\
                                    "You can increase the timeout interval" \
                                    "(UCX_UD_TIMEOUT/UCX_UD_TIMER_BACKOFF/UCX_UD_TIMER_TICK), " \
                                    "or increase the queue depth(UCX_UD_TX_QUEUE_LEN/UCX_UD_RX_QUEUE_LEN) properly.")
            else:
                # LOG_TYPE_RC_TMOUT
                self.logger.log_base("\tIf the number of node errors is evenly distributed, "\
                                    "link layer may be slow. In this case, "\
                                    "You can increase the timeout interval" \
                                    "(UCX_RC_TIMEOUT/UCX_RC_RETRY_COUNT), " \
                                    "or increase the queue depth(UCX_RC_TX_QUEUE_LEN/UCX_RC_RX_QUEUE_LEN) properly.")

        if log_loss == 1:
            self.logger.log_base("Warning: some analysis data is lost, which may lead to analysis result deviation. " \
                                 "The possible cause is that logs are lost or logs are disordered.")

        check_gid = 0
        for host, _ in hostInfos.items():
            for dev in hostInfos[host].get_devs():
                if ':' in dev:
                    self.logger.log_base("Warning: some gid may fail to be parsed, the manual parsing command: " \
                                        "\"ssh <host> ibv_devinfo -v | grep -E 'hca_id|GID|port_lid'\"")
                    check_gid = 1
                    break
            if check_gid:
                break

        self.logger.log_base("*")

    def analyse(self, line):
        for fm in self.log_whitelist:
            result = re.match(fm['format'], line, 0)
            # match
            if result:
                if fm['type'] == LOG_TYPE_UD_TMOUT:
                    self.__analyse_recv_tmout(result)
                if fm['type'] == LOG_TYPE_RC_TMOUT:
                    self.__analyse_recv_tmout(result)
                if fm['type'] == LOG_TYPE_UCP_RECV_TMOUT:
                    self.__analyse_ucp_recv_tmout(result)
                if fm['type'] == LOG_TYPE_PEER_NAME:
                    self.__analyse_peer_name(result)
                self.log_type |= fm['type']
                return

    def dump(self):
        if self.log_type == 0:
            self.logger.log_base("no available log for parsing")
            return
        if self.log_type & LOG_TYPE_UD_TMOUT:
            self.__uct_tmout_dump(LOG_TYPE_UD_TMOUT)
        if self.log_type & LOG_TYPE_RC_TMOUT:
            self.__uct_tmout_dump(LOG_TYPE_RC_TMOUT)
        if self.log_type & LOG_TYPE_UCP_RECV_TMOUT:
            self.__ucp_recv_tmout_dump()


#
# main logic
#


def main():
    print(PyVersion())
    # arg parse
    argParser = ArgParser()
    args = argParser.get_args()
    # parse output file
    log_output = None
    if args.output:
        if not os.path.exists(args.output):
            os.makedirs(args.output)
        if os.path.isdir(args.output):
            try:
                log_output = open(args.output + '/' + OUTPUT_FILE_NAME, 'w')
            except Exception as e:
                print("cannot open output file {}:{}".format(args.output + OUTPUT_FILE_NAME, str(e)))
            if log_output:
                print("output redirect to " + args.output + '/' + OUTPUT_FILE_NAME)
            else:
                print("output rollback to stdout")

    # parse log level
    log_level = argParser.get_log_level()
    # log init
    logger = Logger(level=log_level, f=log_output)
    # analyser init
    # Round down args.upper
    # If args.upper exceeds the range, set it to the default value.
    args.level = int(args.level)
    if args.upper > LIST_OUTPUT_UPPER_MAX or args.upper < LIST_OUTPUT_UPPER_MIN:
        args.upper = LIST_OUTPUT_UPPER_MIN
    logAnalyser = LogAnalyser(logger, args.upper, args.skip_gid_parsing)

    # args check
    try:
        if not args.file and not args.dir:
            argParser.dump_help()
            return
        # If both file and dir exist, only parse file.
        if args.file:
            if not os.path.exists(args.file):
                logger.log_err("log file({}) not exists, please check!".format(args.file))
                return
            if not os.path.isfile(args.file):
                logger.log_err("log file({}) not valid, please check!".format(args.file))
                return
            ret = FileParser(logger, args.file, logAnalyser).parse()
            if not ret:
                return
        elif args.dir:
            if not os.path.exists(args.dir):
                logger.log_err("log dir({}) not exists, please check!".format(args.dir))
                return
            if not os.path.isdir(args.dir):
                logger.log_err("log dir({}) not valid, please check!".format(args.dir))
                return
            ret = DirParser(logger, args.dir, logAnalyser).parse()
            if not ret:
                return
        else:
            # no such branch
            return
        logAnalyser.dump()
        
    finally:
        if log_output:
            log_output.close()


if __name__ == '__main__':
    main()
