#!/usr/bin/python

import json
import subprocess
from prettytable import PrettyTable

def main():
        proc = subprocess.Popen(["kubectl get pods --all-namespaces -o json"], shell=True, stdout=subprocess.PIPE)
        output = proc.stdout.read()
        info = json.loads(output)
        t = PrettyTable(["Namespace", "Node", "Pod", "GPU Type", "GPU Cores"])
        type_gpu_map = dict()
        total_gpu_num = 0
        for pod_info in info["items"]:
                row = checkPod(pod_info)
                if len(row) == 0:
                        continue
                gpu_type, gpu_num = row[3], row[4]
                total_gpu_num += gpu_num
                type_gpu_map[gpu_type] = type_gpu_map.get(gpu_type, 0) + gpu_num
                t.add_row(row)
        print t
        print 'Total GPU: %d' % (total_gpu_num)
        for gpu_type, gpu_num in type_gpu_map.iteritems():
                print '%s\t%d' % (gpu_type, gpu_num)

def checkPod(pod_info):
        try:
                metadata = pod_info["metadata"]
                pod_name = metadata["name"]
                namespace = metadata["namespace"]
                containers = pod_info["spec"]["containers"]
                node_name = pod_info["spec"]["nodeName"]
                total_gpu_num = 0
                for container in containers:
                        if "resources" not in container:
                                continue
                        if "limits" not in container["resources"]:
                                continue
                        gpu_num = int(container["resources"]["limits"].get("alpha.kubernetes.io/nvidia-gpu", 0))
                        total_gpu_num += gpu_num
                if total_gpu_num > 0:
                        gpu_type = getGPUType(pod_info)
                        #print("%s\t%s\t\t\t\t\t%s\t%d" % (namespace, pod_name, gpu_type, total_gpu_num))
                        return [namespace, node_name, pod_name, gpu_type, total_gpu_num]
        except Exception as e:
                print(e)
                return []
        return []

def getGPUType(pod_info):
        gpu_type = "<unspecified>"
        try:
                gpu_type = pod_info["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
        except Exception as e:
                #print('getGPUType err: ', e)
                return gpu_type
        return gpu_type


if __name__ == "__main__":
        main()
