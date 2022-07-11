package memphis

// Factory - a memphis factory object
type Factory struct {
	Name        string
	Description string
	conn        *Conn
}

type createFactoryReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
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

// GetDefaultFactoryOpts - returns default configuration options for the factory.
func GetDefaultFactoryOpts() FactoryOpts {
	return FactoryOpts{
		Description: "",
	}
}

// CreateFactory - creates a factory.
func (c *Conn) CreateFactory(name string, opts ...FactoryOpt) (*Factory, error) {
	defaultOpts := GetDefaultFactoryOpts()

	defaultOpts.Name = name

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, err
			}
		}
	}

	return defaultOpts.CreateFactory(c)
}

// CreateFactory - creates a factory using FactoryOpts struct.
func (opts *FactoryOpts) CreateFactory(c *Conn) (*Factory, error) {
	factory := Factory{Name: opts.Name, Description: opts.Description, conn: c}
	return &factory, c.create(&factory)
}

// Factory.Destroy - destroys this factory.
func (f *Factory) Destroy() error {
	return f.conn.destroy(f)
}

func (f *Factory) getCreationApiPath() string {
	return "/api/factories/createFactory"
}

func (f *Factory) getCreationReq() any {
	return createFactoryReq{
		Name:        f.Name,
		Description: f.Description,
	}
}

func (f *Factory) getDestructionApiPath() string {
	return "/api/factories/removeFactory"
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
