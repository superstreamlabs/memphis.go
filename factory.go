// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memphis

// Factory - a memphis factory object
type Factory struct {
	Name        string
	Description string
	conn        *Conn
}

type createFactoryReq struct {
	Username    string `json:"username"`
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
		Username:    f.conn.username,
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
