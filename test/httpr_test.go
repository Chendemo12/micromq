package test

import (
	"github.com/Chendemo12/micromq/sdk"
	"reflect"
	"testing"
)

func TestSdkHttpProducer_Post(t *testing.T) {
	type fields struct {
		host      string
		port      string
		path      string
		asyncPath string
		token     string
	}
	type args struct {
		topic string
		key   string
		form  any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.ProductResponse
		wantErr bool
	}{
		{
			name: "TestSdkHttpProducer_Post",
			fields: fields{
				host:      "127.0.0.1",
				port:      "7270",
				path:      "/api/edge/product",
				asyncPath: "/api/edge/product/async",
				token:     "123456788",
			},
			args: args{
				topic: "DNS_REPORT",
				key:   "test.test.com",
				form: struct {
					Domain string `json:"domain"`
					IP     string `json:"ip"`
				}{
					Domain: "test.test.com",
					IP:     "10.64.73.28",
				},
			},
			want: &sdk.ProductResponse{
				Status:       "Accepted",
				Offset:       0,
				ResponseTime: 0,
				Message:      "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := sdk.NewHttpProducer(tt.fields.host, tt.fields.port)
			p.SetToken(tt.fields.token)

			got, err := p.Post(tt.args.topic, tt.args.key, tt.args.form)
			if (err != nil) != tt.wantErr {
				t.Errorf("Post() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Post() got = %v, want %v", got, tt.want)
			}
		})
	}
}
