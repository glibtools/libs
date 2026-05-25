package mdb

import (
	"github.com/glibtools/libs/util"
)

type BaseModel struct {
	ID        int      `json:"id,omitempty" gorm:"primaryKey;autoIncrement:true;autoIncrementIncrement:1;"`
	CreatedAt *Time    `json:"created_at,omitempty" gorm:"notnull;default:CURRENT_TIMESTAMP;"`
	UpdatedAt *Time    `json:"updated_at,omitempty" gorm:"notnull;"`
	Version   *Version `json:"version,omitempty" gorm:"notnull;default:1;comment:版本号;"`
}

type Time = util.JSONTime
