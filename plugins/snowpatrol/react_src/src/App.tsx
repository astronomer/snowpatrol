import { useState } from 'react'
import './App.css'
import {
  ChakraProvider,
  Container,
  Stack,
  Box,
  Grid,
  FormControl,
  FormLabel,
  Select,
  Input,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
} from "@chakra-ui/react";

function App() {
  const [count, setCount] = useState(0)

  return (
    <ChakraProvider>
    <div className="card">
        <button onClick={() => setCount((count) => count + 1)}>
          count is {count}
        </button>
        <p>
          Edit <code>src/App.tsx</code> and save to test HMR
        </p>
      </div>
      <Container>
        <Stack spacing={3}>
          <Grid templateColumns="repeat(3, 1fr)" gap={3} alignItems="center">
            <Box>
              <FormControl>
                <FormLabel>Date Range</FormLabel>
                <Input
                  type="date"
                  placeholder="Start Date"
                  variant="outline"
                />
              </FormControl>
            </Box>
            <Box>
              <FormControl>
                <FormLabel>Airflow Dag</FormLabel>
                <Select placeholder="Select Dag" variant="outline" multiple>
                  {/* Add options here */}
                </Select>
              </FormControl>
            </Box>
            <Box>
              <FormControl>
                <FormLabel>Airflow Task</FormLabel>
                <Select placeholder="Select Task" variant="outline" multiple>
                  {/* Add options here */}
                </Select>
              </FormControl>
            </Box>
          </Grid>
          <Table variant="simple">
            <Thead>
              <Tr>
                <Th>Header 1</Th>
                <Th>Header 2</Th>
                <Th>Header 3</Th>
              </Tr>
            </Thead>
            <Tbody>
              {/* Populate table rows with data */}
            </Tbody>
          </Table>
          <Table variant="simple">
            <Thead>
              <Tr>
                <Th>Header 1</Th>
                <Th>Header 2</Th>
                <Th>Header 3</Th>
              </Tr>
            </Thead>
            <Tbody>
              {/* Populate table rows with data */}
            </Tbody>
          </Table>
        </Stack>
      </Container>
    </ChakraProvider>
  );
}

export default App
