package cloud

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

func CreateGCEInstance() {
	//STEP 1 - INIT DATA  - Add all your initialization data e.g. projectid, zones, accessTokens.
	project := "sps-storm"
	zone := "us-central1-c"                                                                                                                                                                     //e.g. europe-west1-d
	accessToken := "ya29.a0Adw1xeUcwYsCPYcGhcFYAKfmKH_iPC7nfiqUDMEZfFyEIHSiu4i2rLd6VrEWo1O0WC-cprPxSiFxzqDc-fWLyyyv_eIsKOkXAcodQX2m2piCjxP1r1v8uQEhUOHdNqkqVse1fcl44dliq08Yg3Vckw_O_Z-pWFGNlik" ////retrieve from https://developers.google.com/oauthplayground/ select relevant compute engine scopes e.g. devstorage/read_write and auth/compute
	endpoint := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances", project, zone)

	//STEP 2 - REQUEST BODY
	//Create a REST representation of whats required. see https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
	reqBody := struct {
		MachineType       string `json:"machineType"`
		Name              string `json:"name"`
		NetworkInterfaces []struct {
			Network string `json:"network"`
		} `json:"networkInterfaces"`
		Disks []struct {
			Boot             bool `json:"boot"`
			AutoDelete       bool `json:"autoDelete"`
			InitializeParams struct {
				DiskName    string `json:"diskName"`
				SourceImage string `json:"sourceImage"`
			} `json:"initializeParams"`
			Mode      string `json:"mode"`
			Interface string `json:"interface"`
		} `json:"disks"`
	}{
		//STEP 3 - Fill out requirements
		Name:        "test-vm",                                                //Whatever name you would like
		MachineType: fmt.Sprintf("zones/%s/machineTypes/n1-standard-1", zone), //Note machine type is not simply n1-standard-1 but the entire path to the resource

		NetworkInterfaces: []struct {
			Network string `json:"network"`
		}{
			{
				Network: fmt.Sprintf("projects/%s/global/networks/default", project),
			},
		}, //For simplicity use the default network
		Disks: []struct {
			Boot             bool `json:"boot"`
			AutoDelete       bool `json:"autoDelete"`
			InitializeParams struct {
				DiskName    string `json:"diskName"`
				SourceImage string `json:"sourceImage"`
			} `json:"initializeParams"`
			Mode      string `json:"mode"`
			Interface string `json:"interface"`
		}{
			{
				Boot:       true,         // The first disk must be a boot disk.
				AutoDelete: false,        //Optional
				Mode:       "READ_WRITE", //Mode should be READ_WRITE or READ_ONLY
				Interface:  "SCSI",       //SCSI or NVME - NVME only for SSDs
				InitializeParams: struct { //Used in place of Source
					DiskName    string `json:"diskName"`
					SourceImage string `json:"sourceImage"`
				}{
					DiskName:    "test-disk-2",
					SourceImage: "projects/debian-cloud/global/images/family/debian-9",
				},
			},
		},
	}

	//STEP 4 - Marshal the request body struct into binary
	bodyData, err := json.Marshal(reqBody)
	if err != nil {
		log.Println(err.Error())
	}

	//STEP 5- BUILD THE REQUEST.
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(bodyData)))
	if err != nil {
		log.Println(err.Error())
	}
	//STEP 6 - ADD necessary headers. The authorization header is MANDATORY
	request.Header.Set(http.CanonicalHeaderKey("authorization"), fmt.Sprintf("Bearer %s", accessToken))
	request.Header.Set(http.CanonicalHeaderKey("content-type"), "application/json")

	//STEP 7 - Use an existing client or a default client ot make the request.
	client := http.DefaultClient
	response, err := client.Do(request)
	if err != nil {
		log.Println(err.Error())
	}

	//STEP 8 - READ RESPONSE AND PRINT
	data, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		log.Println(err.Error())
	}

	fmt.Println(string(data))
}

/*
	OUTPUT
	{
	 "kind": "compute#operation",
	 "id": "<some-id>",
	 "name": "operation-1548831024777-580a7530c89ff-f50206a4-348f6778",
	 "zone": "https://www.googleapis.com/compute/v1/projects/<your-project-here>/zones/europe-west1-d",
	 "operationType": "insert",
	 "targetLink": "https://www.googleapis.com/compute/v1/projects/<your-project-here>/zones/europe-west1-d/instances/test-vm",
	 "targetId": "<some-id>",
	 "status": "PENDING",
	 "user": "<your-email-here>",
	 "progress": 0,
	 "insertTime": "2019-01-29T22:50:26.279-08:00",
	 "selfLink": "https://www.googleapis.com/compute/v1/projects/<your-project-here>/zones/europe-west1-d/operations/operation-1548831024777-580a7530c89ff-f50206a4-348f6778"
	}
*/
