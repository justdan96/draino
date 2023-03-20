package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/DataDog/compute-go/table"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/planetlabs/draino/internal/cli"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
)

type taintCommandFlags struct {
	nodeName           string
	nodegroupName      string
	nodegroupNamespace string
	outputFormat       cli.OutputFormatType
	tableOutputParams  table.OutputParameters
}

var taintCmdFlags taintCommandFlags

func TaintCmd(mgr manager.Manager) *cobra.Command {
	taintCmd := &cobra.Command{
		Use:     "taint",
		Aliases: []string{"taint", "taints"},
		Args:    cobra.MinimumNArgs(1),
		Run:     func(cmd *cobra.Command, args []string) {},
	}
	taintCmd.PersistentFlags().VarP(&taintCmdFlags.outputFormat, "output", "o", "output format (table|json)")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodeName, "node-name", "", "", "name of the node")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupName, "nodegroup-name", "", "", "name of the nodegroup")
	taintCmd.PersistentFlags().StringVarP(&taintCmdFlags.nodegroupNamespace, "nodegroup-namespace", "", "", "namespace of the nodegroup")
	cli.SetTableOutputParameters(&taintCmdFlags.tableOutputParams, taintCmd.PersistentFlags())

	listCmd := &cobra.Command{
		Use:        "list",
		SuggestFor: []string{"list"},
		Args:       cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, err := listNodeFilter(mgr.GetClient(), taintCmdFlags.nodeName, taintCmdFlags.nodegroupName, taintCmdFlags.nodegroupNamespace)
			if err != nil {
				return err
			}
			output, err := FormatNodesOutput(nodes, nil)
			if err != nil {
				return err
			}
			fmt.Println(output)
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:        "delete",
		SuggestFor: []string{"delete"},
		Args:       cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, err := listNodeFilter(mgr.GetClient(), taintCmdFlags.nodeName, taintCmdFlags.nodegroupName, taintCmdFlags.nodegroupNamespace)
			if err != nil {
				return err
			}
			resultColumn := nodeExtraColumns{
				columnName: "Delete",
				data:       map[string]string{},
			}

			fmt.Printf("\nProcessing NLA candidate taint removal\n")
			for _, node := range nodes {
				t, f := k8sclient.GetNLATaint(node)
				if t == nil || !f {
					resultColumn.data[node.Name] = "no taint found"
					continue
				}
				if t.Value != k8sclient.TaintDrainCandidate {
					resultColumn.data[node.Name] = fmt.Sprintf("skipping taint %s", t.Value)
					continue
				}

				if _, err := k8sclient.RemoveNLATaint(context.Background(), mgr.GetClient(), node); err != nil {
					fmt.Printf("%v: %v\n", node.Name, err)
					continue
				}
				fmt.Printf("%v: done\n", node.Name)
			}
			output, err := FormatNodesOutput(nodes, []nodeExtraColumns{resultColumn})
			if err != nil {
				return err
			}

			fmt.Println(output)
			return nil
		},
	}
	taintCmd.AddCommand(listCmd, deleteCmd)
	return taintCmd
}

type nodeExtraColumns struct {
	columnName string
	data       map[string]string
}

func FormatNodesOutput(nodes []*v1.Node, extraColumns []nodeExtraColumns) (string, error) {
	if taintCmdFlags.outputFormat == cli.FormatJSON {
		b, err := json.MarshalIndent(nodes, "", " ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	columns := []string{
		"Name", "NodegroupNamespace", "Nodegroup", "NLATaint",
	}
	for _, ext := range extraColumns {
		columns = append(columns, ext.columnName)
	}
	table := table.NewTable(columns,
		func(obj interface{}) []string {
			node := obj.(*v1.Node)
			ng, ngNs := NGValues(node)
			tValue := ""
			if taint, _ := k8sclient.GetNLATaint(node); taint != nil {
				tValue = taint.Value
			}
			values := []string{
				node.Name,
				ngNs,
				ng,
				tValue,
			}
			for _, ext := range extraColumns {
				values = append(values, ext.data[node.Name])
			}
			return values
		})

	for _, n := range nodes {
		table.Add(n)
	}
	taintCmdFlags.tableOutputParams.Apply(table)
	buf := bytes.NewBufferString("")
	err := table.Display(buf)
	return buf.String(), err
}

func NGValues(node *v1.Node) (name, ns string) {
	return node.Labels[kubernetes.LabelKeyNodeGroupName], node.Labels[kubernetes.LabelKeyNodeGroupNamespace]
}

func listNodeFilter(kclient client.Client, nodeName string, ngName string, ngNamespace string) ([]*v1.Node, error) {

	if nodeName != "" {
		var node v1.Node
		if err := kclient.Get(context.Background(), types.NamespacedName{Name: nodeName}, &node, &client.GetOptions{}); err != nil {
			return nil, err
		}
		if _, f := k8sclient.GetNLATaint(&node); !f {
			return nil, nil
		}

		ng, ngNs := NGValues(&node)
		if ngNamespace != "" && ngNamespace != ngNs {
			return nil, nil
		}
		if ngName != "" && ngName != ng {
			return nil, nil
		}
		return []*v1.Node{&node}, nil
	}
	var result []*v1.Node
	var nodes v1.NodeList
	if err := kclient.List(context.Background(), &nodes, &client.ListOptions{}); err != nil {
		return nil, err
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if _, f := k8sclient.GetNLATaint(node); !f {
			continue
		}

		ng, ngNs := NGValues(node)
		if ngNamespace != "" && ngNamespace != ngNs {
			continue
		}
		if ngName != "" && ngName != ng {
			continue
		}
		result = append(result, node)
	}
	return result, nil
}
