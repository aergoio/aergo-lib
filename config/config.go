/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

/*
Package config provides an easy way to create and manage configurations for aergo projects written in go.

By extending 'IContext' interface, developers can easily use a porject-specific configurations.
It requires to provide base information.

 type IContext interface {
	GetDefaultConfig() interface{} //a default config struct filled with default values
	GetHomePath() string // a default home path
	GetConfigFileName() string // a config file name
	GetTemplate() string // a default toml config file template.
 }

Internally, the pkg has functions to find and manage a configuration file,
and automatically generates toml file using the template, if the file does not exist.
The template is filled with default values of GetDefaultConfig()
The template must follow this format

 string_type = "{{.STRUCT.FILED}}"
 bool/number_type = {{.STRUCT.FILED}}
 string_array_type = [{{range .STRUCT.FILED}}
 "{{.}}", {{end}}
 ]
 bool/number_array_type = [{{range .STRUCT.FILED}}
 {{.}}, {{end}}
 ]
 map = does not support

 [substruct]
 field_in_substruct = ...
 ...

.STRUCT and .FIELD must have a same name with a matched exported struct or field.
You can find a detail example at a 'config_test.go' file.

If there exists multiple configration files, a priorty is
 1. filename given using a flag
 2. home set at env
 3. a default os home dir'
*/
package config

import (
	"bufio"
	"bytes"
	"html/template"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// NewBaseContext returns a BaseContext that is used to load configuration easily.
//
// This functions requests an IContext that contains basic config setting vars.
//
// Other parameters(homePath, configFilePath, envPrefix) are optional.
// If you do not insert them, then this will automatically infer home and config path
// using a context of the given contextImpl. (see retrievePath func)
// If you insert only homePath, then this will try to find a config file at that path.
// Or if you insert only configFilePath, then this will try to open that file.
//
// This only supports a toml extension.
func NewBaseContext(contextImpl IContext, homePath string, configFilePath string, envPrefix string) BaseContext {
	// create new viper conf instance and initialze its environment vars
	viperConf := viper.New()
	viperConf.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viperConf.SetEnvPrefix(envPrefix)
	viperConf.AutomaticEnv()
	viperConf.SetConfigType("toml")

	// set user parameters at the viper conf
	if homePath != "" {
		home, err:= filepath.Abs(homePath)
		if err != nil {
			panic(err)
		}
		viperConf.Set("HOME", home)
	}
	if configFilePath != "" {
		viperConf.SetConfigFile(configFilePath)
	}

	ctx := BaseContext{
		Vc:       viperConf,
		IContext: contextImpl,
	}
	// if user does not specify a configuration file, infer home and default path
	ctx.retrievePath(ctx.Vc)

	return ctx
}

// LoadOrCreateConfig loads a config file using the information in a context.
// If the config file or it's parent directory doesn't exist, then this creats those
// first using a default name and an automatically generated configration text.
func (ctx *BaseContext) LoadOrCreateConfig(defaultConf interface{}) error {

	err := ctx.createHomeAndDefaultConf(ctx.Vc, defaultConf)
	if err != nil {
		return err
	}

	err = ctx.Vc.ReadInConfig()
	if err != nil {
		return err
	}

	err = ctx.Vc.Unmarshal(defaultConf)
	if err != nil {
		return err
	}

	return nil
}

// BindPFlags binds a flagSet, given at a command line, to this context
// and maps to configuration parameters
func (ctx *BaseContext) BindPFlags(flagSet *pflag.FlagSet) {
	ctx.Vc.BindPFlags(flagSet)
}

// retrievePath will get a home path from a given viper conf.
// if the homePath is empty, then fetch home path from an environment
// if environment is empty, then use a default path
// and generate a config path, if an user don't provide it
func (ctx *BaseContext) retrievePath(viperConf *viper.Viper) {
	var confPath string
	if viperConf.ConfigFileUsed() != "" {
		confPath = viperConf.ConfigFileUsed()
	} else if viperConf.IsSet("HOME") {
		homePath := viperConf.GetString("HOME")
		confPath = path.Join(homePath, ctx.GetConfigFileName())
	} else if os.Getenv("HOME") != "" { // for unix
		homePath := path.Join(os.Getenv("HOME"), ctx.GetHomePath())
		confPath = path.Join(homePath, ctx.GetConfigFileName())
		viperConf.Set("HOME", homePath)
	} else if os.Getenv("USERPROFILE") != "" { // for windows
		homePath := path.Join(os.Getenv("USERPROFILE"), ctx.GetHomePath())
		confPath = path.Join(homePath, ctx.GetConfigFileName())
		viperConf.Set("HOME", homePath)
	} else {
		homePath := ctx.GetHomePath()
		confPath = path.Join(homePath, ctx.GetConfigFileName())
		viperConf.Set("HOME", homePath)
	}

	viperConf.SetConfigFile(confPath)
}

// createHomeAndDefaultConf gets a home path and create it if it does not exist.
// and generate default conf text, and write it to a config file
func (ctx *BaseContext) createHomeAndDefaultConf(viperConf *viper.Viper, defaultConf interface{}) error {
	homePath := viperConf.GetString("HOME")
	confPath := viperConf.ConfigFileUsed()

	// check existence of the path
	if homePath != "" {
		if _, err := os.Stat(homePath); os.IsNotExist(err) {
			// create the home directory if it does not exist
			err := os.MkdirAll(homePath, homeDirPermission)
			if err != nil {
				return err
			}
		}
	}

	// create a default config file if it does not exist
	if _, err := os.Open(confPath); os.IsNotExist(err) {
		// generate a config text
		generatedText, err := ctx.fillTemplate(defaultConf)
		if err != nil {
			return err
		}

		// write the genenrated conf text to the file
		err = ioutil.WriteFile(confPath, []byte(generatedText), confFilePermission)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctx *BaseContext) fillTemplate(defaultConf interface{}) (string, error) {
	// parse template of the context
	configFileTemplate, err := template.New("config").Parse(ctx.GetTemplate())
	if err != nil {
		return "", err
	}

	var bbuf bytes.Buffer
	writer := bufio.NewWriter(&bbuf)

	// fill the template text using field values in a defaultConf
	err = configFileTemplate.Execute(writer, defaultConf)
	if err != nil {
		return "", err
	}

	writer.Flush()

	return bbuf.String(), nil
}

// ExpandPathEnv is similar with 'os.ExpandEnv(s string)' func.
// This replace ${var} of $var in a path string according to the values
// of the current environment in the context.
// Undefined values are replaced by the empty string.
func (ctx *BaseContext) ExpandPathEnv(path string) string {

	expandPath := os.Expand(path, func(inputstr string) string {
		return ctx.Vc.GetString(inputstr)
	})

	return filepath.ToSlash(expandPath)
}
