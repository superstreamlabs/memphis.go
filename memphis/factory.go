package memphis

type Factory struct {
	Name        string
	Description string
	conn        *Conn
}

type CreateFactoryReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type RemoveFactoryReq struct {
	Name string `json:"factory_name"`
}

func (c *Conn) CreateFactory(name string, description string) (*Factory, error) {
	factory := Factory{Name: name, Description: description, conn: c}
	return &factory, c.create(&factory)
}

func (f *Factory) Remove() error {
	return f.getConn().destroy(f)
}

func (f *Factory) getCreationApiPath() string {
	return "/api/factories/createFactory"
}

func (f *Factory) getCreationReq() any {
	return CreateFactoryReq{
		Name:        f.Name,
		Description: f.Description,
	}
}

func (f *Factory) getDestructionApiPath() string {
	return "/api/factories/removeFactory"
}

func (f *Factory) getDestructionReq() any {
	return RemoveFactoryReq{Name: f.Name}
}

func (f *Factory) getConn() *Conn {
	return f.conn
}
