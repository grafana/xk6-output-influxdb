/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package influxdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/guregu/null.v3"
)

func TestParseURL(t *testing.T) {
	t.Parallel()
	testdata := map[string]Config{
		"":                                 {Bucket: null.NewString("", false)},
		"bucketname":                       {Bucket: null.StringFrom("bucketname")},
		"/bucketname":                      {Bucket: null.StringFrom("bucketname")},
		"/dbname/retention":                {Bucket: null.StringFrom("dbname/retention")}, // 1.8+ API compatibility
		"http://localhost:8086":            {Addr: null.StringFrom("http://localhost:8086")},
		"http://localhost:8086/bucketname": {Addr: null.StringFrom("http://localhost:8086"), Bucket: null.StringFrom("bucketname")},
	}
	for str, data := range testdata {
		str, data := str, data
		t.Run(str, func(t *testing.T) {
			t.Parallel()
			config, err := parseURL(str)
			assert.NoError(t, err)
			assert.Equal(t, data, config)
		})
	}
}
