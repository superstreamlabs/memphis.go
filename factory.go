// Copyright 2021-2022 The Memphis Authors
// Licensed under the MIT License (the "License");
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// This license limiting reselling the software itself "AS IS".
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package memphis

import "strings"

// Factory - a memphis factory object
type Factory struct {
	Name        string
	Description string
	conn        *Conn
}

type createFactoryReq struct {
	FactoryName string `json:"factory_name"`
	FactoryDesc string `json:"factory_description"`
}

type removeFactoryReq struct {
	Name string `json:"factory_name"`
}

// FactoryOpts - configuration options for a factory.
type FactoryOpts struct {
	Name        string
	Description string
}

// FactoryOpt - a function on the options for a connection.
type FactoryOpt func(*FactoryOpts) error

// getDefaultFactoryOpts - returns default configuration options for the factory.
func getDefaultFactoryOpts() FactoryOpts {
	return FactoryOpts{
		Description: "",
	}
}

// CreateFactory - creates a factory.
func (c *Conn) CreateFactory(name string, opts ...FactoryOpt) (*Factory, error) {
	defaultOpts := getDefaultFactoryOpts()

	defaultOpts.Name = name

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, err
			}
		}
	}
	res, err := defaultOpts.createFactory(c)
	if err != nil && strings.Contains(err.Error(), "already exist") {
		return res, nil
	}
	return res, err
}

// createFactory - creates a factory using FactoryOpts struct.
func (opts *FactoryOpts) createFactory(c *Conn) (*Factory, error) {
	factory := Factory{Name: opts.Name, Description: opts.Description, conn: c}
	return &factory, c.create(&factory)
}

// Factory.Destroy - destroys this factory.
func (f *Factory) Destroy() error {
	return f.conn.destroy(f)
}

func (f *Factory) getCreationSubject() string {
	return "$memphis_factory_creations"
}

func (f *Factory) getCreationReq() any {
	return createFactoryReq{
		FactoryName: f.Name,
		FactoryDesc: f.Description,
	}
}

func (f *Factory) getDestructionSubject() string {
	return "$memphis_factory_destructions"
}

func (f *Factory) getDestructionReq() any {
	return removeFactoryReq{Name: f.Name}
}

// Description - an optional Description of the factory.
func Description(desc string) FactoryOpt {
	return func(opts *FactoryOpts) error {
		opts.Description = desc
		return nil
	}
}
