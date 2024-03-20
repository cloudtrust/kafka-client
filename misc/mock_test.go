package misc

//go:generate mockgen --build_flags=--mod=mod -destination=./mock/oauth2.go -package=mock -mock_names=TokenSource=TokenSource golang.org/x/oauth2 TokenSource
