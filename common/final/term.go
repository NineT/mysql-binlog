package final

import (
	"context"
	"os"
	"runtime/debug"

	"github.com/juju/errors"
	"github.com/zssky/log"
)

/***
终止包用于所有信息的尾声统一处理

不在每个地方都写一套结束逻辑， 避免引发不一致

终止包分两种情况终止： 正常终止， 异常终止
## 正常终止
* 数据处理过程完全正常 没有任何的异常出现 所有都是OK

* 终止处理过程
## 异常处理
* 数据处理过程中出现任何异常　都会调用该异常处理 并终止进程
*/

// Terminate 终止控制 这里用func 是因为需要解除包与包之间的复合引用
func Terminate(err error, f func()) {
	if err != nil {
		// 日志error 级别
		debug.PrintStack()
		log.Error(errors.ErrorStack(err))
	}
	if f != nil {
		// 回调接口
		f()
	}
	os.Exit(-1)
}

// AfterMath means after work
type After struct {
	Errs   chan interface{}   // errors channel
	Ctx    context.Context    // context
	Cancel context.CancelFunc // cancel function
	F      func()             // function before cancel called
}

func (a *After) After() {
	log.Info("after called")
	if err := recover(); err != nil {
		log.Warn("panic error ", err)
		if a.F != nil {
			log.Info("execute custom function ")
			a.F()
		}
		a.Cancel()
	}
}
