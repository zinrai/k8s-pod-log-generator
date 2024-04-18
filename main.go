package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Config struct {
	KubeconfigPath        string `yaml:"kubeconfig_path"`
	NumK8sNamespaces      int    `yaml:"num_k8s_namespaces"`
	BytesPerLogLine       int    `yaml:"bytes_per_log_line"`
	KilobytesPerPodLog    int    `yaml:"kilobytes_per_pod_log"`
	MegabytesTotalLogSize int    `yaml:"megabytes_total_log_size"`
	RunDurationMinutes    int    `yaml:"run_duration_minutes"`
	NamespacePrefix       string `yaml:"namespace_prefix"`
	ConcurrentRequests    int    `yaml:"concurrent_requests"`
}

func calculateTotalLogLines(bytesPerLine int, kilobytesPerLog int) int {
	bytesPerKilobyte := 1024
	return int(math.Ceil(float64(kilobytesPerLog*bytesPerKilobyte) / float64(bytesPerLine)))
}

func calculateTotalPods(megabytesTotalLogSize, kilobytesPerPodLog int) int {
	kilobytesPerMegabyte := 1024
	totalKilobytes := megabytesTotalLogSize * kilobytesPerMegabyte
	return int(math.Ceil(float64(totalKilobytes) / float64(kilobytesPerPodLog)))
}

func createPod(clientset *kubernetes.Clientset, namespace, podName string, totalLogLines, bytesPerLine int) {
	annotations := map[string]string{
		"app":             "k8s-pod-log-generator",
		"total_log_lines": strconv.Itoa(totalLogLines),
	}

	_, err := clientset.CoreV1().Pods(namespace).Create(context.TODO(), &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:  "logger-container",
					Image: "busybox:1.36.1-uclibc",
					Command: []string{
						"/bin/sh",
						"-c",
						fmt.Sprintf("for i in $(seq 1 %d); do cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c %d; echo; done", totalLogLines, bytesPerLine),
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		log.Fatalf("Failed to create Pod %s in namespace %s: %v", podName, namespace, err)
	}
}

func createNamespaces(clientset *kubernetes.Clientset, numK8sNamespaces int, namespacePrefix string) []string {
	namespaces := make([]string, numK8sNamespaces)

	for i := 1; i <= numK8sNamespaces; i++ {
		namespaceName := fmt.Sprintf("%s-%d", namespacePrefix, i)

		_, err := clientset.CoreV1().Namespaces().Get(context.TODO(), namespaceName, metav1.GetOptions{})
		if err == nil {
			err = clientset.CoreV1().Namespaces().Delete(context.TODO(), namespaceName, metav1.DeleteOptions{})
			if err != nil {
				log.Fatalf("Failed to delete existing namespace %s: %v", namespaceName, err)
			}
			log.Printf("Deleted existing namespace %s", namespaceName)

			for {
				_, err = clientset.CoreV1().Namespaces().Get(context.TODO(), namespaceName, metav1.GetOptions{})
				if err != nil {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}

		_, err = clientset.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespaceName,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			log.Fatalf("Failed to create namespace %s: %v", namespaceName, err)
		}
		log.Printf("Namespace %s created", namespaceName)
		namespaces[i-1] = namespaceName
	}

	return namespaces
}

func getRunningPodCount(clientset *kubernetes.Clientset, namespace string) int {
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		FieldSelector: "status.phase!=Succeeded,status.phase!=Failed",
	})
	if err != nil {
		log.Fatalf("Failed to list pods in namespace %s: %v", namespace, err)
	}

	runningPodCount := 0
	for _, pod := range pods.Items {
		if pod.Name != "" && pod.Namespace != "" {
			runningPodCount++
		}
	}

	return runningPodCount
}

func main() {
	configFile := "config.yaml"

	configFileData, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer configFileData.Close()

	var config Config
	decoder := yaml.NewDecoder(configFileData)
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	if config.KubeconfigPath == "" {
		config.KubeconfigPath = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	if config.NamespacePrefix == "" {
		config.NamespacePrefix = "logger-ns"
	}

	totalPods := calculateTotalPods(config.MegabytesTotalLogSize, config.KilobytesPerPodLog)

	totalLogLines := calculateTotalLogLines(config.BytesPerLogLine, config.KilobytesPerPodLog)

	kubeconfig, err := clientcmd.BuildConfigFromFlags("", config.KubeconfigPath)
	if err != nil {
		log.Fatalf("Error building kubeconfig from %s: %v", config.KubeconfigPath, err)
	}

	clientset, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	namespaces := createNamespaces(clientset, config.NumK8sNamespaces, config.NamespacePrefix)

	source := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(source)
	stopTime := time.Now().Add(time.Duration(config.RunDurationMinutes) * time.Minute)
	podIndex := 1

	var wg sync.WaitGroup
	jobQueue := make(chan int, config.ConcurrentRequests)

	for time.Now().Before(stopTime) {
		totalRunningPods := 0
		for _, ns := range namespaces {
			totalRunningPods += getRunningPodCount(clientset, ns)
		}

		if totalRunningPods+config.ConcurrentRequests >= totalPods {
			time.Sleep(5 * time.Second)
			log.Printf("Total running pods reached the target: %d", totalPods)
			continue
		}

		for i := 0; i < config.ConcurrentRequests; i++ {
			jobQueue <- podIndex
			podIndex++
		}

		for i := 0; i < config.ConcurrentRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				sleepTime := rnd.Intn(3) + 1
				time.Sleep(time.Duration(sleepTime) * time.Second)

				podNumber := <-jobQueue
				randomNamespace := namespaces[rnd.Intn(len(namespaces))]
				podName := fmt.Sprintf("logger-pod-%d", podNumber)
				createPod(clientset, randomNamespace, podName, totalLogLines, config.BytesPerLogLine)
				log.Printf("Pod %s in namespace %s created", podName, randomNamespace)
			}()
		}

		wg.Wait()
	}
}
