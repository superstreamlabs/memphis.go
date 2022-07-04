package memphis

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

type FactoryOpts struct {
	Name        string
	Description string
}

type FactoryOpt func(*FactoryOpts) error

func GetDefaultFactoryOpts() FactoryOpts {
	return FactoryOpts{
		Description: "",
	}
}

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

func (opts *FactoryOpts) CreateFactory(c *Conn) (*Factory, error) {
	factory := Factory{Name: opts.Name, Description: opts.Description, conn: c}
	return &factory, c.create(&factory)
}

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

func Description(desc string) FactoryOpt {
	return func(opts *FactoryOpts) error {
		opts.Description = desc
		return nil
	}
}
