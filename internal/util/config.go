package util

import "fmt"
import "github.com/spf13/viper"

func LoadConfig() error {
	viper.SetConfigName("config")
	viper.AddConfigPath("configs")

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("Fatal error config file: %s \n", err)
	}

	return nil
}
