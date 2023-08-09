package edge

import (
	"github.com/Chendemo12/fastapi"
)

var router = fastapi.APIRouter("/edge", []string{"EDGE"})

// Router edge路由组
func Router() *fastapi.Router {
	{
		//router.POST("")
	}

	return router
}
