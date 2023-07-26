package circuitbreaker

// TODO move this to compute-go if ok (already used in DBM and NPD)

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/DataDog/compute-go/version"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"gopkg.in/yaml.v2"
)

const (
	appURLPrefix       = "app."
	DDClientConfigFile = "/etc/config/datadog-client.yaml"
)

type DDClient struct {
	*datadog.APIClient
	Context context.Context
}

func GetDDClient(ctx context.Context, configFile string) (*DDClient, error) {
	cfg := getCfg()
	ddClientConfig, err := getDDClientConfig(configFile)
	if err != nil {
		return nil, err
	}

	ctx, err = getCtx(ctx, ddClientConfig)
	if err != nil {
		return nil, err
	}

	apiClient := datadog.NewAPIClient(cfg)

	return &DDClient{
		APIClient: apiClient,
		Context:   ctx,
	}, nil
}

func getCfg() *datadog.Configuration {
	cfg := datadog.NewConfiguration()
	cfg.UserAgent = fmt.Sprintf("%s/%s %s", version.Service, version.Version, version.Commit)
	return cfg
}

func getCtx(ctx context.Context, cfg *DDClientConfig) (context.Context, error) {
	if cfg.DDurl != "" {
		datadogAPIURL, err := url.ParseRequestURI(cfg.DDurl)
		if err != nil {
			return nil, err
		}
		// the configured datadgog url on our clusters may be configured to start with the app prefix
		// however the `site` parameter does not support that (it prepends an api prefix), so trim the app prefix if present
		// see https://docs.google.com/document/d/1dqd7X8iTqMw3e61C7ucfZ22Uf22_oAv8qDxdhbDKb58/edit#heading=h.ipmfddl3vahq for more information
		host := datadogAPIURL.Host
		if strings.HasPrefix(host, appURLPrefix) {
			host = host[len(appURLPrefix):]
		}
		// set both name and site, both are used depending on the server configuration selected internally in the datadog go client
		ctx = context.WithValue(ctx,
			datadog.ContextServerVariables,
			map[string]string{
				"name":     host,
				"site":     host,
				"protocol": datadogAPIURL.Scheme,
			},
		)
	}

	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: cfg.APIkey,
			},
			"appKeyAuth": {
				Key: cfg.APPkey,
			},
		},
	)
	return ctx, nil
}

// DDClientConfig to access the datadog monitor API
type DDClientConfig struct {
	APIkey string `yaml:"api_key"`
	APPkey string `yaml:"app_key"`
	DDurl  string `yaml:"dd_url"` // default value in the client is "https://api.datadoghq.com"
}

// getConfigMonitorAPI gets all the required env variables
func getDDClientConfig(configFile string) (*DDClientConfig, error) {
	var ddClientConfig DDClientConfig
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("Error while reading configuration '%s': %v", configFile, err)
	}
	err = yaml.Unmarshal(data, &ddClientConfig)
	if err != nil {
		return nil, fmt.Errorf("Error while reading configuration '%s': %v", configFile, err)
	}

	var errStr []string
	if ddClientConfig.APPkey == "" {
		errStr = append(errStr, "Missing APPKey in configuration file "+configFile)
	}
	if len(errStr) != 0 {
		return nil, fmt.Errorf(strings.Join(errStr, "; "))
	}
	return &ddClientConfig, nil
}
