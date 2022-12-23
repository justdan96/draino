package drainbuffer

import (
	"context"
	"encoding/json"
	"errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const CMDataKey = "data"

// Persistor is responsible for persist a given data structure of any kind
type Persistor[T any] interface {
	// Persist writes the given information to the persitent backend
	Persist(*T) error
	// Load tries to retrieve the information from the backend and returns it
	Load() (*T, bool, error)
}

// ConfigMapPersistor is an implementation of the Persistor interface that uses configmap as backend
type ConfigMapPersistor[T any] struct {
	name      string
	namespace string
	client    client.Client
}

// NewConfigMapPersistor creates a new config map persistor instance
func NewConfigMapPersistor[T any](client client.Client, name, namespace string) Persistor[T] {
	return &ConfigMapPersistor[T]{
		name:      name,
		namespace: namespace,
		client:    client,
	}
}

func (p *ConfigMapPersistor[T]) Persist(data *T) error {
	if data == nil {
		return errors.New("data is empty")
	}

	cm, exist, err := p.getConfigMap()
	if err != nil {
		return err
	}

	str, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// If there is no configmap yet, we'll have to create a new one
	if !exist {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      p.name,
				Namespace: p.namespace,
			},
			Data: map[string]string{
				CMDataKey: string(str),
			},
		}
		return p.client.Create(context.Background(), cm)
	}

	// if there is a configmap already, we'll just override the existing entry
	cm.Data[CMDataKey] = string(str)
	return p.client.Update(context.Background(), cm)
}

func (p *ConfigMapPersistor[T]) Load() (*T, bool, error) {
	cm, exist, err := p.getConfigMap()
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, nil
	}

	entry, ok := cm.Data[CMDataKey]
	if !ok {
		return nil, false, nil
	}

	var e T
	err = json.Unmarshal([]byte(entry), &e)
	if err != nil {
		return nil, false, err
	}

	return &e, true, nil
}

func (p *ConfigMapPersistor[T]) getConfigMap() (*corev1.ConfigMap, bool, error) {
	var cm corev1.ConfigMap
	err := p.client.Get(context.Background(), types.NamespacedName{Name: p.name, Namespace: p.namespace}, &cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return &cm, true, nil
}
