package userlogin

import (
	"fmt"
	"github.com/tokopedia/user-dgraph/riderorder"
	"testing"
)

var sampleRequest = []byte(`
{
    "ApproximateCreationDateTime": 1510102680,
    "Keys": {
        "uid": {
            "S": "9387520"
        }
    },
    "NewImage": {
        "Secret": {
            "S": "2S3QI6AMI5EXGGYW"
        },
        "uid": {
            "S": "9387520"
        },
        "c_recent_purchases": {
            "SS": [
                "Kesehatan~Diet & Vitamin~Sistem Kekebalan Tubuh"
            ]
        },
        "session": {
            "S": "na"
        },
        "ip": {
            "S": "202.67.45.38"
        },
        "n_approx_orders": {
            "S": "68"
        },
        "user_data": {
            "M": {
                "digital": {
                    "M": {
                        "category_1": {
                            "M": {
                                "operator_15": {
                                    "M": {
                                        "uuid_1_15_085278883818": {
                                            "M": {
                                                "client_number": {
                                                    "S": "085278883818"
                                                },
                                                "last_product": {
                                                    "S": "75"
                                                },
                                                "last_updated": {
                                                    "S": "20171010105952"
                                                }
                                            }
                                        },
                                        "uuid_1_15_085376301678": {
                                            "M": {
                                                "client_number": {
                                                    "S": "085376301678"
                                                },
                                                "last_product": {
                                                    "S": "75"
                                                },
                                                "last_updated": {
                                                    "S": "20171010105411"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "filtron": {
                    "M": {
                        "uuid_9387520": {
                            "M": {
                                "fingerprint_data": {
                                    "L": [
                                        {
                                            "S": "eyJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzYxLjAuMzE2My4xMDAgU2FmYXJpLzUzNy4zNiIsImxhbmd1YWdlIjoiZW4tVVMiLCJjb2xvcl9kZXB0aCI6MjQsInBpeGVsX3JhdGlvIjoxLjc1LCJoYXJkd2FyZV9jb25jdXJyZW5jeSI6NCwicmVzb2x1dGlvbiI6WzE0NjMsODIzXSwiYXZhaWxhYmxlX3Jlc29sdXRpb24iOlsxNDYzLDc4M10sInRpbWV6b25lX29mZnNldCI6LTQyMCwic2Vzc2lvbl9zdG9yYWdlIjoxLCJsb2NhbF9zdG9yYWdlIjoxLCJpbmRleGVkX2RiIjoxLCJvcGVuX2RhdGFiYXNlIjoxLCJjcHVfY2xhc3MiOiJ1bmtub3duIiwibmF2aWdhdG9yX3BsYXRmb3JtIjoiV2luMzIiLCJkb19ub3RfdHJhY2siOiJ1bmtub3duIiwicmVndWxhcl9wbHVnaW5zIjpbIkNocm9tZSBQREYgUGx1Z2luOjpQb3J0YWJsZSBEb2N1bWVudCBGb3JtYXQ6OmFwcGxpY2F0aW9uL3gtZ29vZ2xlLWNocm9tZS1wZGZ+cGRmIiwiQ2hyb21lIFBERiBWaWV3ZXI6Ojo6YXBwbGljYXRpb24vcGRmfnBkZiIsIk5hdGl2ZSBDbGllbnQ6Ojo6YXBwbGljYXRpb24veC1uYWNsfixhcHBsaWNhdGlvbi94LXBuYWNsfiIsIldpZGV2aW5lIENvbnRlbnQgRGVjcnlwdGlvbiBNb2R1bGU6OkVuYWJsZXMgV2lkZXZpbmUgbGljZW5zZXMgZm9yIHBsYXliYWNrIG9mIEhUTUwgYXVkaW8vdmlkZW8gY29udGVudC4gKHZlcnNpb246IDEuNC44LjEwMDgpOjphcHBsaWNhdGlvbi94LXBwYXBpLXdpZGV2aW5lLWNkbX4iXSwiYWRibG9jayI6ZmFsc2UsImhhc19saWVkX2xhbmd1YWdlcyI6ZmFsc2UsImhhc19saWVkX3Jlc29sdXRpb24iOmZhbHNlLCJoYXNfbGllZF9vcyI6dHJ1ZSwiaGFzX2xpZWRfYnJvd3NlciI6ZmFsc2UsInRvdWNoX3N1cHBvcnQiOlsxMCx0cnVlLHRydWVdLCJqc19mb250cyI6WyJBcmlhbCIsIkFyaWFsIEJsYWNrIiwiQXJpYWwgTmFycm93IiwiQXJpYWwgVW5pY29kZSBNUyIsIkJvb2sgQW50aXF1YSIsIkJvb2ttYW4gT2xkIFN0eWxlIiwiQ2FsaWJyaSIsIkNhbWJyaWEiLCJDYW1icmlhIE1hdGgiLCJDZW50dXJ5IiwiQ2VudHVyeSBHb3RoaWMiLCJDZW50dXJ5IFNjaG9vbGJvb2siLCJDb21pYyBTYW5zIE1TIiwiQ29uc29sYXMiLCJDb3VyaWVyIiwiQ291cmllciBOZXciLCJHYXJhbW9uZCIsIkdlb3JnaWEiLCJIZWx2ZXRpY2EiLCJJbXBhY3QiLCJMdWNpZGEgQnJpZ2h0IiwiTHVjaWRhIENhbGxpZ3JhcGh5IiwiTHVjaWRhIENvbnNvbGUiLCJMdWNpZGEgRmF4IiwiTHVjaWRhIEhhbmR3cml0aW5nIiwiTHVjaWRhIFNhbnMiLCJMdWNpZGEgU2FucyBUeXBld3JpdGVyIiwiTHVjaWRhIFNhbnMgVW5pY29kZSIsIk1pY3Jvc29mdCBTYW5zIFNlcmlmIiwiTW9ub3R5cGUgQ29yc2l2YSIsIk1TIEdvdGhpYyIsIk1TIFBHb3RoaWMiLCJNUyBSZWZlcmVuY2UgU2FucyBTZXJpZiIsIk1TIFNhbnMgU2VyaWYiLCJNUyBTZXJpZiIsIk1ZUklBRCBQUk8iLCJQYWxhdGlubyBMaW5vdHlwZSIsIlNlZ29lIFByaW50IiwiU2Vnb2UgU2NyaXB0IiwiU2Vnb2UgVUkiLCJTZWdvZSBVSSBMaWdodCIsIlNlZ29lIFVJIFNlbWlib2xkIiwiU2Vnb2UgVUkgU3ltYm9sIiwiVGFob21hIiwiVGltZXMiLCJUaW1lcyBOZXcgUm9tYW4iLCJUcmVidWNoZXQgTVMiLCJWZXJkYW5hIiwiV2luZ2RpbmdzIiwiV2luZ2RpbmdzIDIiLCJXaW5nZGluZ3MgMyJdfQ=="
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        },
        "ua": {
            "S": "okhttp/3.8.0"
        },
        "platform": {
            "S": "android"
        },
        "timestamp": {
            "N": "1510102734"
        }
    },
    "OldImage": {
        "Secret": {
            "S": "2S3QI6AMI5EXGGYW"
        },
        "uid": {
            "S": "9387520"
        },
        "c_recent_purchases": {
            "SS": [
                "Kesehatan~Diet & Vitamin~Sistem Kekebalan Tubuh"
            ]
        },
        "session": {
            "S": "na"
        },
        "ip": {
            "S": "202.67.45.38"
        },
        "n_approx_orders": {
            "S": "68"
        },
        "user_data": {
            "M": {
                "digital": {
                    "M": {
                        "category_1": {
                            "M": {
                                "operator_15": {
                                    "M": {
                                        "uuid_1_15_085278883818": {
                                            "M": {
                                                "client_number": {
                                                    "S": "085278883818"
                                                },
                                                "last_product": {
                                                    "S": "75"
                                                },
                                                "last_updated": {
                                                    "S": "20171010105952"
                                                }
                                            }
                                        },
                                        "uuid_1_15_085376301678": {
                                            "M": {
                                                "client_number": {
                                                    "S": "085376301678"
                                                },
                                                "last_product": {
                                                    "S": "75"
                                                },
                                                "last_updated": {
                                                    "S": "20171010105411"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "filtron": {
                    "M": {
                        "uuid_9387520": {
                            "M": {
                                "fingerprint_data": {
                                    "L": [
                                        {
                                            "S": "eyJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzYxLjAuMzE2My4xMDAgU2FmYXJpLzUzNy4zNiIsImxhbmd1YWdlIjoiZW4tVVMiLCJjb2xvcl9kZXB0aCI6MjQsInBpeGVsX3JhdGlvIjoxLjc1LCJoYXJkd2FyZV9jb25jdXJyZW5jeSI6NCwicmVzb2x1dGlvbiI6WzE0NjMsODIzXSwiYXZhaWxhYmxlX3Jlc29sdXRpb24iOlsxNDYzLDc4M10sInRpbWV6b25lX29mZnNldCI6LTQyMCwic2Vzc2lvbl9zdG9yYWdlIjoxLCJsb2NhbF9zdG9yYWdlIjoxLCJpbmRleGVkX2RiIjoxLCJvcGVuX2RhdGFiYXNlIjoxLCJjcHVfY2xhc3MiOiJ1bmtub3duIiwibmF2aWdhdG9yX3BsYXRmb3JtIjoiV2luMzIiLCJkb19ub3RfdHJhY2siOiJ1bmtub3duIiwicmVndWxhcl9wbHVnaW5zIjpbIkNocm9tZSBQREYgUGx1Z2luOjpQb3J0YWJsZSBEb2N1bWVudCBGb3JtYXQ6OmFwcGxpY2F0aW9uL3gtZ29vZ2xlLWNocm9tZS1wZGZ+cGRmIiwiQ2hyb21lIFBERiBWaWV3ZXI6Ojo6YXBwbGljYXRpb24vcGRmfnBkZiIsIk5hdGl2ZSBDbGllbnQ6Ojo6YXBwbGljYXRpb24veC1uYWNsfixhcHBsaWNhdGlvbi94LXBuYWNsfiIsIldpZGV2aW5lIENvbnRlbnQgRGVjcnlwdGlvbiBNb2R1bGU6OkVuYWJsZXMgV2lkZXZpbmUgbGljZW5zZXMgZm9yIHBsYXliYWNrIG9mIEhUTUwgYXVkaW8vdmlkZW8gY29udGVudC4gKHZlcnNpb246IDEuNC44LjEwMDgpOjphcHBsaWNhdGlvbi94LXBwYXBpLXdpZGV2aW5lLWNkbX4iXSwiYWRibG9jayI6ZmFsc2UsImhhc19saWVkX2xhbmd1YWdlcyI6ZmFsc2UsImhhc19saWVkX3Jlc29sdXRpb24iOmZhbHNlLCJoYXNfbGllZF9vcyI6dHJ1ZSwiaGFzX2xpZWRfYnJvd3NlciI6ZmFsc2UsInRvdWNoX3N1cHBvcnQiOlsxMCx0cnVlLHRydWVdLCJqc19mb250cyI6WyJBcmlhbCIsIkFyaWFsIEJsYWNrIiwiQXJpYWwgTmFycm93IiwiQXJpYWwgVW5pY29kZSBNUyIsIkJvb2sgQW50aXF1YSIsIkJvb2ttYW4gT2xkIFN0eWxlIiwiQ2FsaWJyaSIsIkNhbWJyaWEiLCJDYW1icmlhIE1hdGgiLCJDZW50dXJ5IiwiQ2VudHVyeSBHb3RoaWMiLCJDZW50dXJ5IFNjaG9vbGJvb2siLCJDb21pYyBTYW5zIE1TIiwiQ29uc29sYXMiLCJDb3VyaWVyIiwiQ291cmllciBOZXciLCJHYXJhbW9uZCIsIkdlb3JnaWEiLCJIZWx2ZXRpY2EiLCJJbXBhY3QiLCJMdWNpZGEgQnJpZ2h0IiwiTHVjaWRhIENhbGxpZ3JhcGh5IiwiTHVjaWRhIENvbnNvbGUiLCJMdWNpZGEgRmF4IiwiTHVjaWRhIEhhbmR3cml0aW5nIiwiTHVjaWRhIFNhbnMiLCJMdWNpZGEgU2FucyBUeXBld3JpdGVyIiwiTHVjaWRhIFNhbnMgVW5pY29kZSIsIk1pY3Jvc29mdCBTYW5zIFNlcmlmIiwiTW9ub3R5cGUgQ29yc2l2YSIsIk1TIEdvdGhpYyIsIk1TIFBHb3RoaWMiLCJNUyBSZWZlcmVuY2UgU2FucyBTZXJpZiIsIk1TIFNhbnMgU2VyaWYiLCJNUyBTZXJpZiIsIk1ZUklBRCBQUk8iLCJQYWxhdGlubyBMaW5vdHlwZSIsIlNlZ29lIFByaW50IiwiU2Vnb2UgU2NyaXB0IiwiU2Vnb2UgVUkiLCJTZWdvZSBVSSBMaWdodCIsIlNlZ29lIFVJIFNlbWlib2xkIiwiU2Vnb2UgVUkgU3ltYm9sIiwiVGFob21hIiwiVGltZXMiLCJUaW1lcyBOZXcgUm9tYW4iLCJUcmVidWNoZXQgTVMiLCJWZXJkYW5hIiwiV2luZ2RpbmdzIiwiV2luZ2RpbmdzIDIiLCJXaW5nZGluZ3MgMyJdfQ=="
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        },
        "ua": {
            "S": "okhttp/3.8.0"
        },
        "platform": {
            "S": "android"
        },
        "timestamp": {
            "N": "1510094566"
        }
    },
    "SequenceNumber": "328715152700000000003866261082",
    "SizeBytes": 5572,
    "StreamViewType": "NEW_AND_OLD_IMAGES"
}
	`)

func TestSeg(t *testing.T) {
	r := Result{
		User:        []riderorder.User{riderorder.User{UID: "useruid", UserId: "userid"}},
		Fingerprint: []Fingerprint{Fingerprint{UID: "f1uid", Fingerprint_Data: "f1"}},
		PhoneNumber: []riderorder.PhoneNumber{riderorder.PhoneNumber{UID: "p1uid", Phone: "p1"}},
	}
	ph := []string{"p1", "p2"}
	fi := []string{"f1", "f2"}
	m := segregateDGraphData(r, fi, ph, "userid")
	fmt.Println(m)
}
