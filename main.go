package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	nadclient "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	nadinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	nadlister "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	wbclient "github.com/k8snetworkplumbingwg/whereabouts/pkg/client/clientset/versioned"
	wbinformers "github.com/k8snetworkplumbingwg/whereabouts/pkg/client/informers/externalversions"
	wblister "github.com/k8snetworkplumbingwg/whereabouts/pkg/client/listers/whereabouts.cni.cncf.io/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	MULTUS_NET_ANNOTATION = "k8s.v1.cni.cncf.io/networks"
	MAX_RETRIES           = 2
)

type WhereaboutsController struct {
	k8sclient              kubernetes.Interface
	arePodsSynced          cache.InformerSynced
	areIPPoolsSynced       cache.InformerSynced
	areNetAttachDefsSynced cache.InformerSynced
	podsInformer           cache.SharedIndexInformer
	ipPoolInformer         cache.SharedIndexInformer
	netAttachDefInformer   cache.SharedIndexInformer
	podLister              v1corelisters.PodLister
	ipPoolLister           wblister.IPPoolLister
	netAttachDefLister     nadlister.NetworkAttachmentDefinitionLister
	broadcaster            record.EventBroadcaster
	recorder               record.EventRecorder
	addQueue               workqueue.RateLimitingInterface
	delQueue               workqueue.RateLimitingInterface
}

type PodInfo struct {
	operand  string
	podRef   string
	networks []string
}

func NewWhereaboutsController(
	clientset kubernetes.Interface,
	k8sCoreInformerFactory k8sinformers.SharedInformerFactory,
	wbSharedInformerFactory wbinformers.SharedInformerFactory,
	netAttachDefInformerFactory nadinformers.SharedInformerFactory) *WhereaboutsController {

	k8sPodFilteredInformer := k8sCoreInformerFactory.Core().V1().Pods()
	ipPoolInformer := wbSharedInformerFactory.Whereabouts().V1alpha1().IPPools()
	netAttachDefInformer := netAttachDefInformerFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions()

	poolInformer := ipPoolInformer.Informer()
	networksInformer := netAttachDefInformer.Informer()
	podsInformer := k8sPodFilteredInformer.Informer()

	addQueue := workqueue.NewNamedRateLimitingQueue(
		workqueue.DefaultControllerRateLimiter(),
		"whereabouts-add-queue")

	delQueue := workqueue.NewNamedRateLimitingQueue(
		workqueue.DefaultControllerRateLimiter(),
		"whereabouts-del-queue")

	wc := &WhereaboutsController{
		k8sclient:              clientset,
		arePodsSynced:          podsInformer.HasSynced,
		areIPPoolsSynced:       poolInformer.HasSynced,
		areNetAttachDefsSynced: networksInformer.HasSynced,
		podsInformer:           podsInformer,
		ipPoolInformer:         poolInformer,
		netAttachDefInformer:   networksInformer,
		addQueue:               addQueue,
		delQueue:               delQueue,
	}

	podsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    wc.eventAddPod,
		DeleteFunc: wc.eventDeletePod,
	})

	/*
		ipPoolInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: controller.NewPool,
		})

		networksInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: controller.NewNetwork,
		})
	*/
	return nil
}

func (wc *WhereaboutsController) Run(stopCh <-chan struct{}) error {
	defer wc.addQueue.ShutDown()
	defer wc.delQueue.ShutDown()

	zap.S().Infof("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, wc.arePodsSynced, wc.areNetAttachDefsSynced, wc.areIPPoolsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	zap.S().Infof("Starting workers")
	go wait.Until(wc.runAddPodWorker, time.Second, stopCh)
	go wait.Until(wc.runDelPodWorker, time.Second, stopCh)

	zap.S().Infof("Started workers")
	<-stopCh
	zap.S().Infof("Shutting down workers")

	return nil
}

func (wc *WhereaboutsController) runAddPodWorker() {
	for wc.processNextAddPod() {
	}
}

func (wc *WhereaboutsController) processNextAddPod() bool {
	queueItem, shouldQuit := wc.addQueue.Get()
	if shouldQuit {
		return false
	}

	defer wc.addQueue.Done(queueItem)

	podInfo, ok := queueItem.(*PodInfo)
	if !ok {
		wc.addQueue.Forget(queueItem)
		zap.S().Errorf("Expected PodInfo in queue but got %+v", *podInfo)
		return true
	}

	err := wc.AllocateIpForPod(podInfo)
	wc.handleResultAndRequeue(podInfo, wc.addQueue, err)

	return true
}

func (wc *WhereaboutsController) runDelPodWorker() {
	for wc.processNextDelPod() {
	}
}

func (wc *WhereaboutsController) processNextDelPod() bool {
	queueItem, shouldQuit := wc.delQueue.Get()
	if shouldQuit {
		return false
	}

	defer wc.delQueue.Done(queueItem)

	podInfo, ok := queueItem.(*PodInfo)
	if !ok {
		wc.addQueue.Forget(queueItem)
		zap.S().Errorf("Expected PodInfo in queue but got %+v", *podInfo)
		return true
	}

	err := wc.DeleteIpForPod(podInfo)
	wc.handleResultAndRequeue(podInfo, wc.delQueue, err)

	return true
}

func (wc *WhereaboutsController) handleResultAndRequeue(podInfo *PodInfo, queue workqueue.RateLimitingInterface, err error) {
	if err == nil {
		queue.Forget(podInfo)
		return
	}

	currentRetries := queue.NumRequeues(podInfo)
	if currentRetries <= MAX_RETRIES {
		zap.S().Warnf(
			"re-queuing request for pod %+v; retry #: %d",
			*podInfo,
			currentRetries)
		queue.AddRateLimited(podInfo)
		return
	}

	queue.Forget(podInfo)
}

func (wc *WhereaboutsController) AllocateIpForPod(podInfo *PodInfo) error {
	fmt.Printf("Allocated IP for pod %s", podInfo.podRef)
	return nil
}

func (wc *WhereaboutsController) DeleteIpForPod(podInfo *PodInfo) error {
	fmt.Printf("Allocated IP for pod %s", podInfo.podRef)
	return nil
}

func (wc *WhereaboutsController) eventAddPod(obj interface{}) {
	pod, isPod := obj.(*corev1.Pod)
	if !isPod {
		zap.S().Errorf("Received non-Pod object: %+v", obj)
	}

	zap.S().Infof("Got new pod %s\n", pod.ObjectMeta.Name)
	networks, isMultus := GetPodMultusNetworks(pod)
	if !isMultus {
		zap.S().Infof("Pod %s has no multus network annotations, skipping", pod.GetName())
		return
	}
	zap.S().Infof("Has annotations %v", networks)

	podInfo := new(PodInfo)
	podInfo.networks = networks
	podInfo.podRef = composePodRef(pod)
	podInfo.operand = "ADD"

	wc.addQueue.Add(podInfo)
}

func (wc *WhereaboutsController) eventDeletePod(obj interface{}) {
	pod, isPod := obj.(*corev1.Pod)
	if !isPod {
		zap.S().Errorf("Received non-Pod object: %+v", obj)
	}

	zap.S().Infof("Got new pod %s\n", pod.ObjectMeta.Name)
	networks, isMultus := GetPodMultusNetworks(pod)
	if !isMultus {
		zap.S().Infof("Pod %s has no multus network annotations, skipping", pod.GetName())
		return
	}
	zap.S().Infof("Has annotations %v", networks)

	podInfo := new(PodInfo)
	podInfo.networks = networks
	podInfo.podRef = composePodRef(pod)
	podInfo.operand = "DEL"

	wc.delQueue.Add(podInfo)
}

func GetPodMultusNetworks(pod *corev1.Pod) ([]string, bool) {
	annotations := pod.ObjectMeta.Annotations
	if networks, ok := annotations[MULTUS_NET_ANNOTATION]; ok {
		netList := strings.Split(networks, ",")
		return netList, true
	}

	return nil, false
}

func composePodRef(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())
}

func main() {
	kubeconfig := flag.String("kubeconfig", filepath.Join(os.Getenv("HOME"), ".kube", "config"), "absolute path to the kubeconfig file")
	verbose := flag.Bool("verbose", false, "print verbose logs to the console")
	flag.Parse()

	// Initializing zap log with console and file logging support
	if err := configureGlobalLog(*verbose, filepath.Join("/var/log/", "whereaboutsController.log")); err != nil {
		fmt.Printf("log initialization failed: %s", err.Error())
		os.Exit(1)
	}

	stopChan := make(chan struct{})
	defer close(stopChan)

	k8sClientset, err := getClient(*kubeconfig)
	if err != nil {
		zap.S().Fatalf("Failed to get k8s clientset: %s", err)

	}

	nadClientset, err := getNADClient(*kubeconfig)
	if err != nil {
		zap.S().Fatalf("Failed to get k8s clientset: %s", err)

	}

	wbClientset, err := getWhereaboutsClient(*kubeconfig)
	if err != nil {
		zap.S().Fatalf("Failed to get k8s clientset: %s", err)

	}

	podInformerFactory := k8sinformers.NewSharedInformerFactory(k8sClientset, 0)
	ipPoolInformerFactory := wbinformers.NewSharedInformerFactory(wbClientset, 0)
	netAttachDefInformerFactory := nadinformers.NewSharedInformerFactory(nadClientset, 0)

	controller := NewWhereaboutsController(k8sClientset, podInformerFactory, ipPoolInformerFactory, netAttachDefInformerFactory)

	zap.S().Infof("Starting informer factories ...")
	podInformerFactory.Start(stopChan)
	netAttachDefInformerFactory.Start(stopChan)
	ipPoolInformerFactory.Start(stopChan)
	zap.S().Infof("Informer factories started")

	zap.S().Infof("Starting the controller")
	if err = controller.Run(stopChan); err != nil {
		zap.S().Fatalf("error running controller: %s", err)
	}
}

func getClient(kubeconfig string) (*kubernetes.Clientset, error) {
	var kubeClient *kubernetes.Clientset
	if inClusterConfig, err := rest.InClusterConfig(); err != nil {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build config from kubeconfig file: %s", err)
		}
		kubeClient, err = kubernetes.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from config: %s", err)
		}
	} else {
		kubeClient, err = kubernetes.NewForConfig(inClusterConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from in cluster config: %s", err)
		}
	}

	return kubeClient, nil
}

func getNADClient(kubeconfig string) (*nadclient.Clientset, error) {
	var kubeClient *nadclient.Clientset
	if inClusterConfig, err := rest.InClusterConfig(); err != nil {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build config from kubeconfig file: %s", err)
		}
		kubeClient, err = nadclient.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from config: %s", err)
		}
	} else {
		kubeClient, err = nadclient.NewForConfig(inClusterConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from in cluster config: %s", err)
		}
	}

	return kubeClient, nil
}

func getWhereaboutsClient(kubeconfig string) (*wbclient.Clientset, error) {
	var kubeClient *wbclient.Clientset
	if inClusterConfig, err := rest.InClusterConfig(); err != nil {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build config from kubeconfig file: %s", err)
		}
		kubeClient, err = wbclient.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from config: %s", err)
		}
	} else {
		kubeClient, err = wbclient.NewForConfig(inClusterConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to build clientset from in cluster config: %s", err)
		}
	}

	return kubeClient, nil
}

// ConfigureGlobalLog will log debug to console, else would put logs
// in the home directory
func configureGlobalLog(debugConsole bool, logFile string) error {

	// use lumberjack for log rotation
	f := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    100, // mb
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	})
	// This seems pretty complicated, but all we are doing is making sure
	// error level is reported on console and console looks more 'production' like
	// whereas our log file is more like development log with stack traces etc.

	devEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	prodEncoder := zapcore.NewConsoleEncoder(getProdEncoderConfig())

	// consoleLog will go to stderr
	consoleLogs := zapcore.Lock(os.Stderr)
	// all the logs to file
	fileLogs := zapcore.Lock(f)
	// by default on console we will only print panic error, unless debugConsole is specified
	consoleLvl := zap.PanicLevel
	if debugConsole {
		consoleLvl = zap.DebugLevel
	}

	core := zapcore.NewTee(
		zapcore.NewCore(prodEncoder, consoleLogs, consoleLvl),
		zapcore.NewCore(devEncoder, fileLogs, zap.DebugLevel),
	)

	logger := zap.New(core)
	defer logger.Sync()
	// use the logger we created globally
	zap.ReplaceGlobals(logger)
	// Now start the logging business
	zap.S().Debug("Logger started")
	return nil
}

func getProdEncoderConfig() zapcore.EncoderConfig {
	prodcfg := zap.NewProductionEncoderConfig()
	// by default production encoder has epoch time, using something more readable
	prodcfg.EncodeTime = zapcore.ISO8601TimeEncoder
	return prodcfg
}
