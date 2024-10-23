package misc

import (
	"errors"
	"testing"

	"github.com/cloudtrust/kafka-client/misc/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/oauth2"
)

func TestTokenProvider(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockTokenProvider = mock.NewTokenSource(mockCtrl)

	var clientID = "clientID"
	var clientSecret = "clientSecret"
	var tokenURL = "https://token.url/"
	t.Run("Constructor", func(t *testing.T) {
		var tp = NewTokenProvider(clientID, clientSecret, tokenURL)
		assert.NotNil(t, tp)
	})
	t.Run("Token failure", func(t *testing.T) {
		var tp = &TokenProvider{
			tokenSource: mockTokenProvider,
		}
		var anError = errors.New("an error")
		mockTokenProvider.EXPECT().Token().Return(nil, anError)
		var _, err = tp.Token()
		assert.Equal(t, anError, err)
	})
	t.Run("Token success", func(t *testing.T) {
		var tp = &TokenProvider{
			tokenSource: mockTokenProvider,
		}
		var aToken = &oauth2.Token{
			AccessToken: "a-token-only-valid-for-tests",
		}
		mockTokenProvider.EXPECT().Token().Return(aToken, nil)
		var res, err = tp.Token()
		assert.Nil(t, err)
		assert.Equal(t, aToken.AccessToken, res.Token)
	})
}
