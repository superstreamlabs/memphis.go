package memphis

type CreateFactoryReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type RemoveFactoryReq struct {
	Name string `json:"factory_name"`
}

func (c *Conn) CreateFactory(name string, description string) (Factory, error) {
	return Factory{Name: name, conn: c},
		c.managementRequest("POST", "/api/factories/createFactory", CreateFactoryReq{Name: name, Description: description})
}

func (c *Conn) RemoveFactory(name string) error {
	return c.managementRequest("DELETE", "/api/factories/removeFactory", RemoveFactoryReq{Name: name})
}

func (f *Factory) Remove() error {
	return f.conn.RemoveFactory(f.Name)
}

type Factory struct {
	Name string
	conn *Conn
}
