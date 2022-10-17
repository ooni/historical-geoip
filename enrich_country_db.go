package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type ASOrgMap map[string][][]string

func getASMeta(asn uint64, asOrgMap ASOrgMap, dayStr string) (string, string, string) {
	// See: https://datatracker.ietf.org/doc/html/rfc5398 & https://datatracker.ietf.org/doc/html/rfc6793
	if (asn >= 64496 && asn <= 64511) || (asn >= 65536 && asn <= 65551) {
		return "Reserved for use in documentation and sample code", "ZZ", ""
	}

	// See: https://datatracker.ietf.org/doc/html/rfc1930 & https://datatracker.ietf.org/doc/html/rfc6996
	if (asn >= 64512 && asn <= 65534) || (asn >= 4200000000 && asn <= 4294967294) {
		return "Reserved for private use", "ZZ", ""
	}

	// See: https://datatracker.ietf.org/doc/html/rfc7300
	if (asn == 65535) || (asn >= 65552 && asn <= 131071) || (asn == 4294967295) {
		return "Reserved", "ZZ", ""
	}

	metaList := asOrgMap[fmt.Sprintf("%d", asn)]
	if len(metaList) == 0 {
		log.Printf("Missing ASN for %d", asn)
		return "Unassigned", "ZZ", ""
	}
	meta := metaList[0]
	for _, p := range metaList {
		if p[2] > dayStr {
			break
		}
		meta = p
	}
	return meta[0], meta[1], meta[3]
}

func main() {
	dayStr := flag.String("dayStr", "", "Date timestamp to build geoip database for (format: %Y%m%d)")
	dbFile := flag.String("dbFile", "", "the location of the geoip database file to enrich")
	outputFile := flag.String("outputFile", "", "the location of the geoip database file to write")
	asOrgMapPath := flag.String("asOrgMap", "outputs/all_as_org_map.json", "location of the AS to org map file")
	prefix2asDir := flag.String("prefix2asDir", "cache_dir/routeviews-prefix2as", "location of prefix2as files")

	flag.Parse()

	if *dayStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("[+] Building geoip-countryASN DB for day: %s", *dayStr)

	asOrgMapFile, err := os.Open(*asOrgMapPath)
	if err != nil {
		log.Fatal(err)
	}
	defer asOrgMapFile.Close()

	asOrgMapBytes, err := ioutil.ReadAll(asOrgMapFile)
	if err != nil {
		log.Fatal(err)
	}
	var asOrgMap ASOrgMap
	json.Unmarshal(asOrgMapBytes, &asOrgMap)

	writer, err := mmdbwriter.New(mmdbwriter.Options{DatabaseType: "GeoLite2-ASN", RecordSize: 24})
	if err != nil {
		log.Fatal(err)
	}

	if *dbFile != "" {
		writer, err = mmdbwriter.Load(*dbFile, mmdbwriter.Options{})
		if err != nil {
			log.Fatal(err)
		}
	}

	ipClasses := []string{"rv2", "rv6"}
	for _, ipClass := range ipClasses {
		prefix2asFile, err := os.Open(fmt.Sprintf("%s/routeviews-%s-%s.pfx2as.gz", *prefix2asDir, ipClass, *dayStr))
		if err != nil {
			log.Fatal(err)
		}
		defer prefix2asFile.Close()

		gzipReader, err := gzip.NewReader(prefix2asFile)
		if err != nil {
			log.Fatal(err)
		}
		defer gzipReader.Close()

		scanner := bufio.NewScanner(gzipReader)
		for scanner.Scan() {
			line := scanner.Text()
			p := strings.Split(line, "\t")
			// In the case of multi origin or as-sets we just take the first AS in the
			// list.
			// FWIW this looks like it's the same approach taken by the
			// m-lab/annotation-service: https://github.com/m-lab/annotation-service/blob/master/asn/asn-annotator.go#L63
			asnStr := strings.Split(strings.Split(p[2], "_")[0], ",")[0]
			asn, err := strconv.ParseUint(asnStr, 10, 32)
			if err != nil {
				log.Printf("Invalid ASN %s", p[2])
				log.Fatal(err)
			}
			_, sreNet, err := net.ParseCIDR(fmt.Sprintf("%s/%s", p[0], p[1]))
			if err != nil {
				log.Printf("Invalid net %s/%s", p[0], p[1])
				log.Fatal(err)
			}

			org_name, org_cc, as_name := getASMeta(asn, asOrgMap, *dayStr)
			_ = org_cc

			sreData := mmdbtype.Map{
				"autonomous_system_number":       mmdbtype.Uint32(asn),
				"autonomous_system_organization": mmdbtype.String(org_name),
				"autonomous_system_country":      mmdbtype.String(org_cc),
				"autonomous_system_name":         mmdbtype.String(as_name),
			}

			if err := writer.InsertFunc(sreNet, inserter.TopLevelMergeWith(sreData)); err != nil {
				log.Printf("Failed to insert %v", sreData)
				log.Print(err)
			}
		}
	}

	fh, err := os.Create(*outputFile)
	if err != nil {
		log.Fatal(err)
	}
	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}
}
