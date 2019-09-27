package http

import (
	"context"
	"fmt"
	"io/ioutil"
	http "net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NewMockUserBackend returns a UserBackend with mock services.
func NewMockUserBackend() *UserBackend {
	return &UserBackend{
		Logger:                  zap.NewNop().With(zap.String("handler", "user")),
		UserService:             mock.NewUserService(),
		UserOperationLogService: mock.NewUserOperationLogService(),
		PasswordsService:        mock.NewPasswordsService("", ""),
	}
}

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	userBackend := NewMockUserBackend()
	userBackend.HTTPErrorHandler = ErrorHandler(0)
	userBackend.UserService = svc
	handler := NewUserHandler(userBackend)
	server := httptest.NewServer(handler)
	client := UserService{
		Addr:     server.URL,
		OpPrefix: inmem.OpPrefix,
	}

	done := server.Close

	return &client, inmem.OpPrefix, done
}

func TestUserService(t *testing.T) {
	t.Parallel()
	platformtesting.UserService(initUserService, t)
}

func TestService_handleGetUser(t *testing.T) {
	type fields struct {
		UserService platform.UserService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a user with no status defaults to `active`",
			fields: fields{
				&mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.User{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "myname",
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/users/020f755c3c082000",
		    "logs": "/api/v2/users/020f755c3c082000/logs"
		  },
		  "id": "020f755c3c082000",
		  "name": "myname",
		  "status": "active"
		}
		`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userBackend := NewMockUserBackend()
			userBackend.HTTPErrorHandler = ErrorHandler(0)
			userBackend.UserService = tt.fields.UserService
			h := NewUserHandler(userBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetUser(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)
			t.Logf(res.Header.Get("X-Influx-Error"))

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetUser() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetUser() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetUser(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetUser() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}
