package fxapp

import (
	"testing"

	"go.uber.org/fx"
)

func TestGraph(t *testing.T) {
	if err := fx.ValidateApp(Options()); err != nil {
		t.Fatal(err)
	}
}
