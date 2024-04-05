package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
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
		"app": "k8s-pod-log-generator",
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

func createRandomPods(clientset *kubernetes.Clientset, namespaces []string, totalLogLines, bytesPerLine, totalPods int) {
	source := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(source)

	for i := 1; i <= totalPods; i++ {
		randomNamespace := namespaces[rnd.Intn(len(namespaces))]
		podName := fmt.Sprintf("logger-pod-%d", i)
		podRemaining := totalPods - i
		createPod(clientset, randomNamespace, podName, totalLogLines, bytesPerLine)
		log.Printf("Pod: %s , Namespace: %s , Remaining: %d", podName, randomNamespace, podRemaining)
	}
}

func createNamespaces(clientset *kubernetes.Clientset, numK8sNamespaces int) []string {
	namespaces := make([]string, numK8sNamespaces)

	for i := 1; i <= numK8sNamespaces; i++ {
		namespaceName := fmt.Sprintf("logger-ns%d", i)
		_, err := clientset.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
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

	namespaces := createNamespaces(clientset, config.NumK8sNamespaces)

	createRandomPods(clientset, namespaces, totalLogLines, config.BytesPerLogLine, totalPods)
}
