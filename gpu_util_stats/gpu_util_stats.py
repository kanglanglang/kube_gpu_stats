#!/bin/python
# -*- coding: utf-8 -*-

import json
import requests
from datetime import datetime, timedelta
from time import mktime

prom_base_url = 'http://prometheus.ke-xs-sys.qiniu.io/api/v1'
proxies = {
        'http': 'http://10.34.33.80'
}

def query_prom(query, start_time, end_time, step_sec):
        url = prom_base_url + '/query_range'
        start_time_unix_ts = int(mktime(start_time.timetuple()))
        end_time_unix_ts = int(mktime(end_time.timetuple()))
        payload = {
                'query': query,
                'start': start_time_unix_ts,
                'end': end_time_unix_ts,
                'step': step_sec,
        }
        r = requests.get(url, params=payload, timeout=5, proxies=proxies)
        json_result = r.json()
        #print json.dumps(json_result, indent=2)
        return json_result

def query_prom_instant(query):
        url = prom_base_url + '/query'
        payload = {'query': query}
        r = requests.get(url, params=payload, timeout=5)
        json_result = r.json()
        return json_result

def stats_server_results(json_result, time_range_sec, step_sec):
        server_cards = get_gpu_servers()
        server_val = dict()
        server_val_dict = dict()                # server => value list
        server_pod_val_dict = dict()    # server => pod_list => value list
        results = json_result['data']['result']
        # server 粒度
        for res in results:
                #server = '%s(%s)' % (res['metric']['kubernetes_io_hostname'], res['metric']['nvidia_gpu_type'])
                server = res['metric']['kubernetes_io_hostname']
                val_list = []
                for val in res['values']:
                        val_list.append(float(val[1]))
                server_val_dict[server] = val_list
        for server, val_list in server_val_dict.iteritems():
                val_avg = sum(val_list) * step_sec / time_range_sec
                server_val[server] = val_avg

        lines = []
        for server in sorted(server_cards.keys()):
                val = server_val[server] if server in server_val else 0
                (total, used, card) = server_cards[server]
                line = [server, card, val, used, total]
                lines.append(line)
        return lines

def stats_pod_results(json_result, start_time, end_time):
        server_cards = get_gpu_servers()
        server_pods = get_pod_by_servers(start_time, end_time)
        server_pod_val_dict = dict()    # server => pod_list => value list
        results = json_result['data']['result']
        # server + pod 粒度
        for res in results:
                #server = '%s(%s)' % (res['metric']['kubernetes_io_hostname'], res['metric']['nvidia_gpu_type'])
                server = res['metric']['kubernetes_io_hostname']
                pod = res['metric']['pod_name']
                val_list = []
                for val in res['values']:
                        val_list.append(float(val[1]))
                if server not in server_pod_val_dict:
                        server_pod_val_dict[server] = dict()
                if len(val_list) > 0:
                        server_pod_val_dict[server][pod] = sum(val_list) / len(val_list)
                else:
                        server_pod_val_dict[server][pod] = 0.0

        lines = []
        for server in sorted(server_cards.keys()):
                if server not in server_pods and server not in server_pod_val_dict:
                        continue
                for pod in set(server_pods.get(server, {}).keys() + server_pod_val_dict.get(server, {}).keys()):
                        card_num = server_pods.get(server, {}).get(pod)
                        # 会存在已经结束的，但是有GPU使用率的Pod，暂不统计
                        if card_num == None:
                                continue
                        util = server_pod_val_dict.get(server, {}).get(pod, 0.0)
                        lines.append([server, pod, card_num, util])

        return lines

def get_gpu_servers():
        result_dict = dict() # server -> (total_cards, used_cards, card_type)
        server_card = dict()
        server_total = dict()
        server_used = dict()

    # get total cards
        url = prom_base_url + '/query'
        payload = {
                'query': 'max(kube_node_status_allocatable_nvidia_gpu_cards * on (instance, node) group_left(label_nvidia_gpu_type) kube_node_labels) by (node,label_nvidia_gpu_type)'
        }
        r = requests.get(url, params=payload, timeout=5, proxies=proxies)
        total_cards_result = r.json()['data']['result']
        for res in total_cards_result:
                cards = res['value'][1]
                server_card[res['metric']['node']] = res['metric']['label_nvidia_gpu_type']
                server_total[res['metric']['node']] = cards

        # get used card
        url = prom_base_url + '/query'
        payload = {
                'query': '(sum(max(kube_pod_container_resource_requests_nvidia_gpu_devices{node!=""} * on (instance,pod)  group_right(node) kube_pod_status_phase{phase="Running"}) by (node,pod)) by (node) > 0) * on (node) group_right kube_node_labels'
        }
        r = requests.get(url, params=payload, timeout=5, proxies=proxies)
        used_cards_result = r.json()['data']['result']
        for res in used_cards_result:
                cards = res['value'][1]
                server_used[res['metric']['node']] = cards

        for server in server_total.keys():
                result_dict[server] = (server_total[server], server_used.get(server, 0), server_card.get(server, ''))
        return result_dict

def get_pod_by_servers(start_time, end_time):
        result_dict = dict() # server -> pod -> value
        pod_set = set()

        pod_status_results = query_prom_instant('max(kube_pod_status_phase{namespace="ava",phase=~"Running|Pending"}) by (pod) > 0')
        for m in pod_status_results['data']['result']:
                pod_set.add(m['metric']['pod'])

        gpu_server_pod_results = query_prom('max(kube_pod_container_resource_requests_nvidia_gpu_devices) by (node, pod)', start_time, end_time, 3600)
        #print json.dumps(gpu_server_pod_results, indent=2)
        for m in gpu_server_pod_results['data']['result']:
                server = m['metric'].get('node', '<unknown>')
                pod = m['metric'].get('pod', '<unknown>')
                val = m['values'][0][1]
                # filter by pod status
                if pod not in pod_set:
                        continue
                if server not in result_dict:
                        result_dict[server] = dict()
                result_dict[server][pod] = val

        print json.dumps(result_dict, indent=2)
        return result_dict


def main():
        step = 3600
        time_range = timedelta(days=1)
        end_time = datetime.now()
        start_time = end_time - time_range
        results = query_prom('avg(container_gpu_sm_util) by (kubernetes_io_hostname, nvidia_gpu_type, pod_name)', start_time, end_time, step)
        #stats_server_results(results, time_range_sec, step)
        lines = stats_pod_results(results, start_time, end_time)
        for line in lines:
                print line

if __name__ == '__main__':
        main()
