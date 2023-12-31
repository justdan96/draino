package drainbuffer

import (
	"testing"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"k8s.io/client-go/tools/record"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	fake "k8s.io/utils/clock/testing"
)

type StaticClock struct {
	time time.Time
}

func TestDrainBuffer(t *testing.T) {
	tests := []struct {
		Name        string
		Clock       *fake.FakeClock
		DrainBuffer time.Duration

		Step       time.Duration
		ShouldFind bool
	}{
		{
			Name:        "shlould find entry in configmap as it's not expired yet",
			Clock:       fake.NewFakeClock(time.Now()),
			DrainBuffer: time.Minute,

			Step:       time.Second * 10,
			ShouldFind: true,
		},
		{
			Name:        "should not find entry anymore, because it's expired",
			Clock:       fake.NewFakeClock(time.Now()),
			DrainBuffer: time.Minute,

			Step:       time.Minute * 2,
			ShouldFind: false,
		},
	}

	logger := logr.Discard()
	cmName := "foobar"
	cmNS := "default"

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fakeClient := fakeclient.NewSimpleClientset()

			// Récupère un pointeur sur l'interface de gestion des ConfigMaps
			configMapClient := fakeClient.CoreV1().ConfigMaps(cmNS)

			// initial setup
			ctx, cancel := context.WithCancel(context.Background())
			persistor := NewConfigMapPersistor(configMapClient, cmName, cmNS)
			recorder := kubernetes.NewEventRecorder(record.NewFakeRecorder(1000))
			interf := NewDrainBuffer(ctx, persistor, tt.Clock, logger, recorder, nil, nil, time.Second)
			drainBuffer := interf.(*drainBufferImpl)
			drainBuffer.Initialize(ctx)

			// create new entry
			err := drainBuffer.StoreDrainAttempt("foobar", tt.DrainBuffer)
			assert.NoError(t, err, "failed to store successful drain")
			err = drainBuffer.cleanupAndPersist(ctx)
			assert.NoError(t, err, "cannot persist drain buffer")
			// Close old buffer cleanup
			cancel()

			// Create new buffer to test load mechanism
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			persistor = NewConfigMapPersistor(configMapClient, cmName, cmNS)
			interf = NewDrainBuffer(ctx, persistor, tt.Clock, logger, recorder, nil, nil, time.Second)
			drainBuffer = interf.(*drainBufferImpl)

			// Move clock forward & trigger cleanup
			tt.Clock.Step(tt.Step)
			drainBuffer.Initialize(ctx)
			drainBuffer.cleanupCache()

			// Check if clock is properly set
			next, err := drainBuffer.NextDrain("foobar")
			assert.NoError(t, err, "failed to get next drain for group")
			if tt.ShouldFind {
				until := tt.Clock.Now().Add(-tt.Step).Add(tt.DrainBuffer)
				assert.True(t, next.Before(until.Add(time.Second)))
				assert.True(t, next.After(until.Add(-time.Second)))
			} else {
				assert.True(t, next.IsZero(), "should not find entry")
			}
		})
	}
}
