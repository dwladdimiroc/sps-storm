package util

import (
	"github.com/jszwec/csvutil"
	"github.com/spf13/viper"
	"os"
	"strings"
)

func csvHeaderData(line string) string {
	if s := strings.Split(line, "\n"); len(s) > 0 {
		return s[0]
	} else {
		return "error"
	}
}

func csvGetData(line string) string {
	if s := strings.Split(line, "\n"); len(s) > 0 {
		return s[1]
	} else {
		return "error"
	}
}

func CreateCsv(filename string, data interface{}) error {
	if f, err := os.Create(viper.GetString("storm.csv") + filename + ".csv"); err != nil {
		return err
	} else {
		if b, err := csvutil.Marshal(data); err != nil {
			return err
		} else {
			headerCsv := csvHeaderData(string(b))
			if _, err := f.Write([]byte(headerCsv + "\n")); err != nil {
				return err
			} else {
				if err := f.Close(); err != nil {
					return err
				} else {
					return nil
				}
			}
		}
	}
}

func WriteCsv(filename string, data interface{}) error {
	if b, err := csvutil.Marshal(data); err != nil {
		return err
	} else {
		if f, err := os.OpenFile(viper.GetString("storm.csv")+filename+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			return err
		} else {
			defer f.Close()
			sData := csvGetData(string(b))
			if _, err := f.WriteString(sData + "\n"); err != nil {
				return err
			} else {
				return nil
			}
		}
	}
}
